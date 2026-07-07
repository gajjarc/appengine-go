package taskqueue

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/internal"
)

type PendingCloudTask struct {
	QueueName string    `datastore:"queue_name"`
	TaskName  string    `datastore:"task_name"`
	Payload   string    `datastore:"payload,noindex"`
	Created   time.Time `datastore:"created"`
	Status    string    `datastore:"status"`
}

var (
	pendingTasksMu sync.Mutex
	pendingTasks   = make(map[uint64][]string) // transaction handle -> list of urlsafe keys
	taskNameRegex  = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

func init() {
	internal.PostCommitHook = func(ctx context.Context, handle uint64) {
		go dispatchPendingTasks(ctx, handle)
	}
	internal.RollbackHook = func(handle uint64) {
		cleanupPendingTasks(handle)
	}
	http.HandleFunc("/_ah/cloudtask/sweep", handleSweep)
}

func cleanupPendingTasks(handle uint64) {
	pendingTasksMu.Lock()
	delete(pendingTasks, handle)
	pendingTasksMu.Unlock()
}

type noCancelContext struct {
	context.Context
}

func (c *noCancelContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (c *noCancelContext) Done() <-chan struct{} {
	return nil
}

func (c *noCancelContext) Err() error {
	return nil
}

func logErrorf(ctx context.Context, format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

func dispatchPendingTasks(ctx context.Context, handle uint64) {
	pendingTasksMu.Lock()
	urlsafeKeys, ok := pendingTasks[handle]
	if ok {
		delete(pendingTasks, handle)
	}
	pendingTasksMu.Unlock()

	if !ok || len(urlsafeKeys) == 0 {
		return
	}

	noCancelCtx := &noCancelContext{Context: internal.TransactionlessContext(ctx)}

	for _, urlsafeKey := range urlsafeKeys {
		key, err := datastore.DecodeKey(urlsafeKey)
		if err != nil {
			logErrorf(ctx, "Failed to decode pending task key: %v", err)
			continue
		}

		var taskEntity PendingCloudTask
		err = datastore.Get(noCancelCtx, key, &taskEntity)
		if err != nil {
			logErrorf(ctx, "Failed to get pending task from Datastore: %v", err)
			continue
		}

		err = sendRESTTask(noCancelCtx, taskEntity.QueueName, taskEntity.TaskName, taskEntity.Payload)
		if err != nil {
			if err == ErrTaskAlreadyAdded {
				datastore.Delete(noCancelCtx, key)
				continue
			}
			logErrorf(ctx, "Failed to dispatch task %s to queue %s: %v", taskEntity.TaskName, taskEntity.QueueName, err)
			continue
		}

		err = datastore.Delete(noCancelCtx, key)
		if err != nil {
			logErrorf(ctx, "Failed to delete pending task %s from Datastore: %v", taskEntity.TaskName, err)
		}
	}
}

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

func sendRESTTask(ctx context.Context, queueName string, taskName string, jsonPayload string) error {
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

	url := fmt.Sprintf("https://cloudtasks.googleapis.com/v2beta3/projects/%s/locations/%s/queues/%s/tasks", project, region, queueName)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return ErrTaskAlreadyAdded
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cloud tasks REST returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
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
	if taskName == "" {
		taskName = "task-" + generateUUID()
	}

	fullTaskName := fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", project, region, queueName, taskName)

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
	if _, ok := headers["X-AppEngine-TaskName"]; !ok {
		headers["X-AppEngine-TaskName"] = taskName
	}

	targetService := extractServiceFromHost(ctx, headers["Host"])
	routing := map[string]string{
		"service": targetService,
	}

	aeReq := map[string]interface{}{
		"httpMethod":       task.method(),
		"relativeUri":      path,
		"headers":          headers,
		"appEngineRouting": routing,
	}

	if len(task.Payload) > 0 {
		aeReq["body"] = base64.StdEncoding.EncodeToString(task.Payload)
	}

	taskMap := map[string]interface{}{
		"name":                 fullTaskName,
		"appEngineHttpRequest": aeReq,
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

func buildRESTPayload(ctx context.Context, queueName string, task *Task) (string, string, error) {
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

	payload, taskName, err := buildRESTPayload(ctx, queueName, task)
	if err != nil {
		return nil, err
	}

	if t := internal.TransactionFromContext(ctx); t != nil {
		key := datastore.NewIncompleteKey(ctx, "_AE_PendingCloudTask", nil)
		pendingTask := &PendingCloudTask{
			QueueName: queueName,
			TaskName:  taskName,
			Payload:   payload,
			Created:   time.Now(),
			Status:    "PENDING",
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

	err = sendRESTTask(ctx, queueName, taskName, payload)
	if err != nil {
		return nil, err
	}

	resultTask := *task
	resultTask.Name = taskName
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

	chunkSize := 100
	for chunkStart := 0; chunkStart < len(tasks); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(tasks) {
			chunkEnd = len(tasks)
		}
		chunkTasks := tasks[chunkStart:chunkEnd]

		requests := make([]map[string]interface{}, 0, len(chunkTasks))
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

			requests = append(requests, map[string]interface{}{
				"parent": fullQueueName,
				"task":   taskMap,
			})
		}
		if len(requests) == 0 {
			continue
		}

		batchPayload, err := json.Marshal(map[string]interface{}{"requests": requests})
		if err != nil {
			for i := range chunkTasks {
				if me[chunkStart+i] == nil {
					me[chunkStart+i] = err
					any = true
				}
			}
			continue
		}

		url := fmt.Sprintf("https://cloudtasks.googleapis.com/v2beta3/%s/tasks:batchCreate", fullQueueName)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(batchPayload))
		if err != nil {
			for i := range chunkTasks {
				if me[chunkStart+i] == nil {
					me[chunkStart+i] = err
					any = true
				}
			}
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			for i := range chunkTasks {
				if me[chunkStart+i] == nil {
					me[chunkStart+i] = err
					any = true
				}
			}
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			continue
		} else if resp.StatusCode == http.StatusConflict {
			for i := range chunkTasks {
				if me[chunkStart+i] == nil {
					me[chunkStart+i] = ErrTaskAlreadyAdded
					any = true
				}
			}
		} else {
			err := fmt.Errorf("cloud tasks REST batchCreate returned status %d: %s", resp.StatusCode, string(respBody))
			for i := range chunkTasks {
				if me[chunkStart+i] == nil {
					me[chunkStart+i] = err
					any = true
				}
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

		batchPayload, err := json.Marshal(map[string]interface{}{"names": names})
		if err != nil {
			for i := range chunkTasks {
				me[chunkStart+i] = err
				any = true
			}
			continue
		}

		url := fmt.Sprintf("https://cloudtasks.googleapis.com/v2beta3/%s/tasks:batchDelete", fullQueueName)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(batchPayload))
		if err != nil {
			for i := range chunkTasks {
				me[chunkStart+i] = err
				any = true
			}
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			for i := range chunkTasks {
				me[chunkStart+i] = err
				any = true
			}
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
			continue
		} else {
			err := fmt.Errorf("cloud tasks REST batchDelete returned status %d: %s", resp.StatusCode, string(respBody))
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

func sweep(ctx context.Context) error {
	query := datastore.NewQuery("_AE_PendingCloudTask")
	var tasks []PendingCloudTask
	keys, err := query.GetAll(ctx, &tasks)
	if err != nil {
		return fmt.Errorf("failed to query _AE_PendingCloudTask: %v", err)
	}

	now := time.Now()
	count := 0
	for i, key := range keys {
		task := tasks[i]
		if !task.Created.IsZero() && now.Sub(task.Created) < 60*time.Second {
			continue
		}

		err := sendRESTTask(ctx, task.QueueName, task.TaskName, task.Payload)
		if err != nil && err != ErrTaskAlreadyAdded {
			logErrorf(ctx, "Sweeper failed to dispatch task %s: %v", task.TaskName, err)
			continue
		}

		if err := datastore.Delete(ctx, key); err != nil {
			logErrorf(ctx, "Sweeper failed to delete entity %s: %v", task.TaskName, err)
		}
		count++
	}

	log.Printf("Cloud Tasks sweeper processed %d tasks.", count)
	return nil
}

func handleSweep(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	if err := sweep(ctx); err != nil {
		logErrorf(ctx, "Sweeper failed: %v", err)
		http.Error(w, fmt.Sprintf("Sweeper failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Sweeper completed successfully.\n"))
}
