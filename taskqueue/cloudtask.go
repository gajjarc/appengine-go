package taskqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/internal"
	pb "google.golang.org/appengine/internal/taskqueue"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2beta3"
	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"google.golang.org/api/option"
	"golang.org/x/oauth2"
)

var taskNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type metadataToken struct {
	AccessToken string `json:"access_token"`
}

func getAccessToken(ctx context.Context) (string, error) {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var token metadataToken
	if err := json.Unmarshal(body, &token); err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func getRegion(ctx context.Context) (string, error) {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/region", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.TrimSpace(string(body)), "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid region format: %s", string(body))
	}
	return parts[len(parts)-1], nil
}

func sendTask(ctx context.Context, queueName string, taskName string, jsonPayload string) (string, error) {
	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}

	region, err := getRegion(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get region: %v", err)
	}

	token, err := getAccessToken(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get access token: %v", err)
	}

	opts := []option.ClientOption{}
	if token != "" {
		opts = append(opts, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})))
	}

	client, err := cloudtasks.NewClient(ctx, opts...)
	if err != nil {
		return "", fmt.Errorf("failed to create cloudtasks client: %v", err)
	}
	defer client.Close()

	parent := fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, region, queueName)

	var taskMap map[string]interface{}
	_ = json.Unmarshal([]byte(jsonPayload), &taskMap)

	req := &taskspb.CreateTaskRequest{
		Parent: parent,
	}

	if t, ok := taskMap["task"].(map[string]interface{}); ok {
		taskObj := &taskspb.Task{}
		if name, ok := t["name"].(string); ok && name != "" {
			taskObj.Name = name
		}
		if aeReq, ok := t["app_engine_http_request"].(map[string]interface{}); ok {
			ae := &taskspb.AppEngineHttpRequest{}
			if method, ok := aeReq["http_method"].(string); ok {
				if code, ok := taskspb.HttpMethod_value[method]; ok {
					ae.HttpMethod = taskspb.HttpMethod(code)
				}
			}
			if uri, ok := aeReq["relative_uri"].(string); ok {
				ae.RelativeUri = uri
			}
			if headers, ok := aeReq["headers"].(map[string]interface{}); ok {
				ae.Headers = make(map[string]string)
				for k, v := range headers {
					if strV, ok := v.(string); ok {
						ae.Headers[k] = strV
					}
				}
			}
			if bodyStr, ok := aeReq["body"].(string); ok {
				if bodyBytes, err := base64.StdEncoding.DecodeString(bodyStr); err == nil {
					ae.Body = bodyBytes
				} else {
					ae.Body = []byte(bodyStr)
				}
			}
			if routing, ok := aeReq["app_engine_routing"].(map[string]interface{}); ok {
				ae.AppEngineRouting = &taskspb.AppEngineRouting{}
				if svc, ok := routing["service"].(string); ok {
					ae.AppEngineRouting.Service = svc
				}
			}
			taskObj.PayloadType = &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: ae}
		}
		req.Task = taskObj
	}

	createdTask, err := client.CreateTask(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") || strings.Contains(err.Error(), "409") {
			return "", ErrTaskAlreadyAdded
		}
		return "", err
	}
	shortName := taskName
	if createdTask != nil && createdTask.Name != "" {
		if idx := strings.LastIndex(createdTask.Name, "/"); idx != -1 {
			shortName = createdTask.Name[idx+1:]
		} else {
			shortName = createdTask.Name
		}
	}
	return shortName, nil
}

func extractServiceFromHost(ctx context.Context, host string) string {
	if host == "" {
		if s := os.Getenv("GAE_SERVICE"); s != "" {
			return s
		}
		return "default"
	}

	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}

	// Find the domain suffix starting from the project ID
	pIdx := strings.Index(host, project)
	if pIdx == -1 {
		// Fallback to defaultHost check if project ID not found in host
		defaultHost := appengine.DefaultVersionHostname(ctx)
		if host == defaultHost {
			return "default"
		}
		return host
	}

	domainSuffix := host[pIdx:]
	if host == domainSuffix {
		return "default"
	}

	suffixes := []string{
		"." + domainSuffix,
		"-dot-" + domainSuffix,
	}
	stripped := host
	for _, suffix := range suffixes {
		if strings.HasSuffix(stripped, suffix) {
			stripped = stripped[:len(stripped)-len(suffix)]
			break
		}
	}

	if stripped == host {
		return host
	}

	stripped = strings.ReplaceAll(stripped, "-dot-", ".")
	parts := strings.Split(stripped, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return "default"
}

func buildTaskMap(ctx context.Context, queueName string, task *Task) (map[string]interface{}, string, error) {
	if task.Name != "" {
		if !taskNameRegex.MatchString(task.Name) {
			return nil, "", fmt.Errorf("taskqueue: invalid task name %q", task.Name)
		}
	}

	if len(task.Payload) > 100*1024 {
		return nil, "", fmt.Errorf("taskqueue: task too large (%d bytes)", len(task.Payload))
	}

	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}

	region, err := getRegion(ctx)
	if err != nil {
		return nil, "", err
	}

	taskName := task.Name
	var fullTaskName string
	if taskName != "" {
		fullTaskName = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", project, region, queueName, taskName)
	}

	path := task.Path
	if path == "" {
		path = "/_ah/queue/" + queueName
	}

	headers := make(map[string]string)
	for k, vs := range task.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	if _, ok := headers["Content-Type"]; !ok {
		headers["Content-Type"] = "application/octet-stream"
	}
	if _, ok := headers["X-AppEngine-QueueName"]; !ok {
		headers["X-AppEngine-QueueName"] = queueName
	}
	if taskName != "" {
		if _, ok := headers["X-AppEngine-TaskName"]; !ok {
			headers["X-AppEngine-TaskName"] = taskName
		}
	}

	targetService := extractServiceFromHost(ctx, headers["Host"])
	routing := map[string]string{
		"service": targetService,
	}

	aeReq := map[string]interface{}{
		"http_method":        task.method(),
		"relative_uri":       path,
		"headers":             headers,
		"app_engine_routing": routing,
	}

	if len(task.Payload) > 0 {
		aeReq["body"] = base64.StdEncoding.EncodeToString(task.Payload)
	}

	taskMap := map[string]interface{}{
		"app_engine_http_request": aeReq,
	}
	if fullTaskName != "" {
		taskMap["name"] = fullTaskName
	}

	eta := task.ETA
	if eta.IsZero() {
		if task.Delay > 0 {
			eta = time.Now().Add(task.Delay)
		}
	}
	if !eta.IsZero() {
		taskMap["scheduleTime"] = eta.UTC().Format(time.RFC3339Nano)
	}

	if task.RetryOptions != nil {
		rc := make(map[string]interface{})
		ro := task.RetryOptions
		if ro.RetryLimit > 0 {
			rc["maxAttempts"] = ro.RetryLimit + 1
		}
		if ro.AgeLimit > 0 {
			rc["maxRetryDuration"] = fmt.Sprintf("%.3fs", ro.AgeLimit.Seconds())
		}
		if ro.MinBackoff > 0 {
			rc["minBackoff"] = fmt.Sprintf("%.3fs", ro.MinBackoff.Seconds())
		}
		if ro.MaxBackoff > 0 {
			rc["maxBackoff"] = fmt.Sprintf("%.3fs", ro.MaxBackoff.Seconds())
		}
		if ro.MaxDoublings > 0 || (ro.MaxDoublings == 0 && ro.ApplyZeroMaxDoublings) {
			rc["maxDoublings"] = ro.MaxDoublings
		}
		if len(rc) > 0 {
			taskMap["retryConfig"] = rc
		}
	}

	return taskMap, taskName, nil
}

func serializeTaskPayload(ctx context.Context, queueName string, task *Task) (string, string, error) {
	taskMap, taskName, err := buildTaskMap(ctx, queueName, task)
	if err != nil {
		return "", "", err
	}

	reqMap := map[string]interface{}{
		"task": taskMap,
	}

	jsonBytes, err := json.Marshal(reqMap)
	if err != nil {
		return "", "", err
	}

	return string(jsonBytes), taskName, nil
}

func addInCloudTasks(ctx context.Context, task *Task, queueName string) (*Task, error) {
	if queueName == "" {
		queueName = "default"
	}

	payload, taskName, err := serializeTaskPayload(ctx, queueName, task)
	if err != nil {
		return nil, err
	}

	if t := internal.TransactionFromContext(ctx); t != nil {
		key := datastore.NewIncompleteKey(ctx, "_AE_PendingCloudTask", nil)
		pendingTask := &PendingCloudTask{
			QueueName:        queueName,
			CloudTaskName:    taskName,
			CloudTaskPayload: payload,
			Created:          time.Now(),
			Status:           "PENDING",
			RetryCount:       0,
			LastError:        "",
			HandledBySweeper: false,
			SdkLang:          "GO",
		}
		key, err = datastore.Put(ctx, key, pendingTask)
		if err != nil {
			return nil, fmt.Errorf("failed to save transactional task to Datastore: %v", err)
		}

		handle := t.GetHandle()
		pendingTasksMu.Lock()
		pendingTasks[handle] = append(pendingTasks[handle], key.Encode())
		pendingTasksMu.Unlock()

		resultTask := *task
		resultTask.Name = taskName
		resultTask.Method = task.method()
		return &resultTask, nil
	}

	assignedName, err := sendTask(ctx, queueName, taskName, payload)
	if err != nil {
		return nil, err
	}

	resultTask := *task
	resultTask.Name = assignedName
	resultTask.Method = task.method()
	return &resultTask, nil
}

func addMultiInCloudTasks(ctx context.Context, tasks []*Task, queueName string) ([]*Task, error) {
	if internal.TransactionFromContext(ctx) != nil {
		me, any := make(appengine.MultiError, len(tasks)), false
		results := make([]*Task, len(tasks))
		for i, task := range tasks {
			res, err := addInCloudTasks(ctx, task, queueName)
			if err != nil {
				me[i] = err
				any = true
			} else {
				results[i] = res
			}
		}
		if any {
			return results, me
		}
		return results, nil
	}

	if queueName == "" {
		queueName = "default"
	}
	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}
	region, err := getRegion(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get region: %v", err)
	}
	token, err := getAccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get access token: %v", err)
	}
	fullQueueName := fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, region, queueName)

	me, any := make(appengine.MultiError, len(tasks)), false
	results := make([]*Task, len(tasks))

	opts := []option.ClientOption{}
	if token != "" {
		opts = append(opts, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})))
	}

	client, err := cloudtasks.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create cloudtasks client: %v", err)
	}
	defer client.Close()

	chunkSize := 100
	for chunkStart := 0; chunkStart < len(tasks); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(tasks) {
			chunkEnd = len(tasks)
		}
		chunkTasks := tasks[chunkStart:chunkEnd]

		createReqs := make([]*taskspb.CreateTaskRequest, 0, len(chunkTasks))
		for i, t := range chunkTasks {
			taskMap, taskName, err := buildTaskMap(ctx, queueName, t)
			if err != nil {
				me[chunkStart+i] = err
				any = true
				continue
			}
			results[chunkStart+i] = new(Task)
			*results[chunkStart+i] = *t
			results[chunkStart+i].Name = taskName
			results[chunkStart+i].Method = t.method()

			taskObj := &taskspb.Task{}
			if taskName != "" {
				taskObj.Name = fmt.Sprintf("%s/tasks/%s", fullQueueName, taskName)
			}
			if aeReq, ok := taskMap["app_engine_http_request"].(map[string]interface{}); ok {
				ae := &taskspb.AppEngineHttpRequest{}
				if method, ok := aeReq["http_method"].(string); ok {
					if code, ok := taskspb.HttpMethod_value[method]; ok {
						ae.HttpMethod = taskspb.HttpMethod(code)
					}
				}
				if uri, ok := aeReq["relative_uri"].(string); ok {
					ae.RelativeUri = uri
				}
				if headers, ok := aeReq["headers"].(map[string]interface{}); ok {
					ae.Headers = make(map[string]string)
					for k, v := range headers {
						if strV, ok := v.(string); ok {
							ae.Headers[k] = strV
						}
					}
				}
				if bodyStr, ok := aeReq["body"].(string); ok {
					ae.Body = []byte(bodyStr)
				}
				taskObj.PayloadType = &taskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: ae}
			}

			createReqs = append(createReqs, &taskspb.CreateTaskRequest{
				Parent: fullQueueName,
				Task:   taskObj,
			})
		}
		if len(createReqs) == 0 {
			continue
		}

		batchReq := &taskspb.BatchCreateTasksRequest{
			Parent:   fullQueueName,
			Requests: createReqs,
		}

		_, err = client.BatchCreateTasks(ctx, batchReq)
		if err != nil {
			if strings.Contains(err.Error(), "Unimplemented") || strings.Contains(err.Error(), "unknown method") || strings.Contains(err.Error(), "404") {
				for i, t := range chunkTasks {
					if me[chunkStart+i] != nil {
						continue
					}
					res, err := addInCloudTasks(ctx, t, queueName)
					if err != nil {
						me[chunkStart+i] = err
						any = true
					} else {
						results[chunkStart+i] = res
					}
				}
			} else {
				parseOperationErrors([]byte(err.Error()), len(chunkTasks), chunkStart, me, &any, false)
			}
		}
	}

	if any {
		return results, me
	}
	return results, nil
}

func deleteMultiInCloudTasks(ctx context.Context, tasks []*Task, queueName string) error {
	if queueName == "" {
		queueName = "default"
	}
	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}
	region, err := getRegion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get region: %v", err)
	}
	token, err := getAccessToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to get access token: %v", err)
	}
	fullQueueName := fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, region, queueName)

	opts := []option.ClientOption{}
	if token != "" {
		opts = append(opts, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})))
	}

	client, err := cloudtasks.NewClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create cloudtasks client: %v", err)
	}
	defer client.Close()

	me, any := make(appengine.MultiError, len(tasks)), false

	chunkSize := 1000
	for chunkStart := 0; chunkStart < len(tasks); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(tasks) {
			chunkEnd = len(tasks)
		}
		chunkTasks := tasks[chunkStart:chunkEnd]

		names := make([]string, len(chunkTasks))
		for i, t := range chunkTasks {
			names[i] = fmt.Sprintf("%s/tasks/%s", fullQueueName, t.Name)
		}

		batchReq := &taskspb.BatchDeleteTasksRequest{
			Parent: fullQueueName,
			Names:  names,
		}

		_, err = client.BatchDeleteTasks(ctx, batchReq)
		if err != nil {
			for i := range chunkTasks {
				me[chunkStart+i] = err
				any = true
			}
		}
	}

	if any {
		return me
	}
	return nil
}



type operationResponse struct {
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Metadata *struct {
		FailedRequests      map[string]struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"failedRequests"`
		FailedRequestsSnake map[string]struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"failed_requests"`
	} `json:"metadata"`
}

func parseOperationErrors(respBody []byte, totalTasks int, chunkStart int, me appengine.MultiError, any *bool, isDelete bool) {
	var opResp operationResponse
	if err := json.Unmarshal(respBody, &opResp); err != nil {
		return
	}
	if opResp.Error != nil && opResp.Error.Code != 0 {
		err := mapOperationErrorCode(opResp.Error.Code, opResp.Error.Message, isDelete)
		for i := 0; i < totalTasks; i++ {
			if me[chunkStart+i] == nil {
				me[chunkStart+i] = err
				*any = true
			}
		}
		return
	}
	if opResp.Metadata != nil {
		failedReqs := opResp.Metadata.FailedRequests
		if len(failedReqs) == 0 {
			failedReqs = opResp.Metadata.FailedRequestsSnake
		}
		for idxStr, fail := range failedReqs {
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err == nil && idx >= 0 && idx < totalTasks {
				if me[chunkStart+idx] == nil {
					me[chunkStart+idx] = mapOperationErrorCode(fail.Code, fail.Message, isDelete)
					*any = true
				}
			}
		}
	}
}

func mapOperationErrorCode(code int, msg string, isDelete bool) error {
	if isDelete && (code == 5 || code == 404 || strings.Contains(strings.ToLower(msg), "not found") || strings.Contains(strings.ToLower(msg), "unknown")) {
		return &internal.APIError{
			Service: "taskqueue",
			Detail:  msg,
			Code:    int32(pb.TaskQueueServiceError_UNKNOWN_TASK),
		}
	}
	if code == 6 || code == 409 || strings.Contains(strings.ToLower(msg), "already exists") || ((code == 5 || code == 404) && strings.Contains(strings.ToLower(msg), "requested entity was not found")) {
		return ErrTaskAlreadyAdded
	}
	if code == 5 || code == 404 {
		return &internal.APIError{
			Service: "taskqueue",
			Detail:  msg,
			Code:    int32(pb.TaskQueueServiceError_UNKNOWN_TASK),
		}
	}
	return fmt.Errorf("cloud tasks operation failed (%d): %s", code, msg)
}
