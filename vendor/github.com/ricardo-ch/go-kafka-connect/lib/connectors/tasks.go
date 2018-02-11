package connectors

import (
	"fmt"
	"net/http"
	"strconv"
)

//TaskRequest is generic request when interacting with task endpoint
type TaskRequest struct {
	Connector string
	TaskID    int
}

//GetAllTasksResponse is response to get all tasks of a specific endpoint
type GetAllTasksResponse struct {
	Code  int
	Tasks []TaskDetails
}

//TaskDetails is detail of a specific task on a specific endpoint
type TaskDetails struct {
	ID     TaskID            `json:"id"`
	Config map[string]string `json:"config"`
}

//TaskID identify a task and its connector
type TaskID struct {
	Connector string `json:"connector"`
	TaskID    int    `json:"task"`
}

//TaskStatusResponse is response returned by get task status endpoint
type TaskStatusResponse struct {
	Code   int
	Status TaskStatus
}

//TaskStatus define task status
type TaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

//GetAllTasks return list of running task
func (c Client) GetAllTasks(req ConnectorRequest) (GetAllTasksResponse, error) {
	var gatr GetAllTasksResponse
	var taskDetails []TaskDetails

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s/tasks", req.Name), nil, &taskDetails)
	if err != nil {
		return GetAllTasksResponse{}, err
	}

	gatr.Code = statusCode
	gatr.Tasks = taskDetails
	return gatr, nil
}

//GetTaskStatus return current status of task
func (c Client) GetTaskStatus(req TaskRequest) (TaskStatusResponse, error) {
	var tsr TaskStatusResponse

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s/tasks/%s/status", req.Connector, strconv.Itoa(req.TaskID)), nil, &tsr)
	if err != nil {
		return TaskStatusResponse{}, err
	}

	tsr.Code = statusCode

	return tsr, nil
}

//RestartTask try to restart task
func (c Client) RestartTask(req TaskRequest) (EmptyResponse, error) {
	var er EmptyResponse

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s/tasks/%s/restart", req.Connector, strconv.Itoa(req.TaskID)), nil, nil)
	if err != nil {
		return EmptyResponse{}, err
	}

	er.Code = statusCode

	return er, nil
}
