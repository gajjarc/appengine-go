package taskqueue

import (
	"context"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2beta3"
	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
)

var taskNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func getQueuePath(ctx context.Context, queueName string) (string, error) {
	if queueName == "" {
		queueName = "default"
	}
	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}
	region, err := getRegion(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get region: %v", err)
	}
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, region, queueName), nil
}

func sendTask(ctx context.Context, queueName string, taskName string, taskObj *taskspb.Task) (string, error) {
	parent, err := getQueuePath(ctx, queueName)
	if err != nil {
		return "", err
	}

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create cloudtasks client: %v", err)
	}
	defer client.Close()

	req := &taskspb.CreateTaskRequest{
		Parent: parent,
		Task:   taskObj,
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

	pIdx := strings.Index(host, project)
	if pIdx == -1 {
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

func buildCloudTaskProto(ctx context.Context, queueName string, task *Task) (*taskspb.Task, string, error) {
	if task.Name != "" {
		if !taskNameRegex.MatchString(task.Name) {
			return nil, "", fmt.Errorf("taskqueue: invalid task name %q", task.Name)
		}
	}

	if len(task.Payload) > 100*1024 {
		return nil, "", fmt.Errorf("taskqueue: task too large (%d bytes)", len(task.Payload))
	}

	queuePath, err := getQueuePath(ctx, queueName)
	if err != nil {
		return nil, "", err
	}

	taskName := task.Name
	var fullTaskName string
	if taskName != "" {
		fullTaskName = fmt.Sprintf("%s/tasks/%s", queuePath, taskName)
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
	ae := &taskspb.AppEngineHttpRequest{
		RelativeUri: path,
		Headers:     headers,
		Body:        task.Payload,
		AppEngineRouting: &taskspb.AppEngineRouting{
			Service: targetService,
		},
	}
	if code, ok := taskspb.HttpMethod_value[task.method()]; ok {
		ae.HttpMethod = taskspb.HttpMethod(code)
	}

	taskObj := &taskspb.Task{
		Name: fullTaskName,
		PayloadType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: ae,
		},
	}

	if !task.ETA.IsZero() {
		taskObj.ScheduleTime = timestamppb.New(task.ETA)
	} else if task.Delay > 0 {
		taskObj.ScheduleTime = timestamppb.New(time.Now().Add(task.Delay))
	}

	return taskObj, taskName, nil
}

func addInCloudTasks(ctx context.Context, task *Task, queueName string) (*Task, error) {
	if queueName == "" {
		queueName = "default"
	}

	taskObj, taskName, err := buildCloudTaskProto(ctx, queueName, task)
	if err != nil {
		return nil, err
	}

	if t := internal.TransactionFromContext(ctx); t != nil {
		protoBytes, err := proto.Marshal(taskObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal proto for transactional task: %v", err)
		}
		key := datastore.NewIncompleteKey(ctx, "_AE_PendingCloudTask", nil)
		pendingTask := &PendingCloudTask{
			QueueName:        queueName,
			CloudTaskName:    taskName,
			CloudTaskPayload: string(protoBytes),
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

	assignedName, err := sendTask(ctx, queueName, taskName, taskObj)
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

	fullQueueName, err := getQueuePath(ctx, queueName)
	if err != nil {
		return nil, err
	}

	me, any := make(appengine.MultiError, len(tasks)), false
	results := make([]*Task, len(tasks))

	client, err := cloudtasks.NewClient(ctx)
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
			taskObj, taskName, err := buildCloudTaskProto(ctx, queueName, t)
			if err != nil {
				me[chunkStart+i] = err
				any = true
				continue
			}
			results[chunkStart+i] = new(Task)
			*results[chunkStart+i] = *t
			results[chunkStart+i].Name = taskName
			results[chunkStart+i].Method = t.method()

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
	fullQueueName, err := getQueuePath(ctx, queueName)
	if err != nil {
		return err
	}

	client, err := cloudtasks.NewClient(ctx)
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
