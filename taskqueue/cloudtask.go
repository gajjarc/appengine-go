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
	defaultHost := appengine.DefaultVersionHostname(ctx)
	if host == defaultHost {
		return "default"
	}

	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	suffixes := []string{
		"." + defaultHost,
		"-dot-" + defaultHost,
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

func buildRESTPayload(ctx context.Context, queueName string, task *Task) (string, string, error) {
	if task.Name != "" {
		if !taskNameRegex.MatchString(task.Name) {
			return "", "", fmt.Errorf("taskqueue: invalid task name %q", task.Name)
		}
	}

	if len(task.Payload) > 100*1024 {
		return "", "", fmt.Errorf("taskqueue: task too large (%d bytes)", len(task.Payload))
	}

	project := appengine.AppID(ctx)
	if idx := strings.Index(project, "~"); idx != -1 {
		project = project[idx+1:]
	}

	region, err := getRegion(ctx)
	if err != nil {
		return "", "", err
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
		key := datastore.NewIncompleteKey(ctx, "_PendingCloudTask", nil)
		pendingTask := &PendingCloudTask{
			QueueName: queueName,
			TaskName:  taskName,
			Payload:   payload,
			Created:   time.Now(),
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
