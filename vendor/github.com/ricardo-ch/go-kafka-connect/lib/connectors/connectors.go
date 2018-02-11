package connectors

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

//ConnectorRequest is generic request used when interacting with connector endpoint
type ConnectorRequest struct {
	Name string `json:"name"`
}

//CreateConnectorRequest is request used for creating connector
type CreateConnectorRequest struct {
	ConnectorRequest
	Config map[string]string `json:"config"`
}

//GetAllConnectorsResponse is request used to get list of available connectors
type GetAllConnectorsResponse struct {
	Code       int
	Connectors []string
	ErrorResponse
}

//ConnectorResponse is generic response when interacting with connector endpoint
type ConnectorResponse struct {
	Code   int
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
	Tasks  []TaskID          `json:"tasks"`
	ErrorResponse
}

//GetConnectorConfigResponse is response returned by GetConfig endpoint
type GetConnectorConfigResponse struct {
	Code   int
	Config map[string]string
	ErrorResponse
}

//GetConnectorStatusResponse is response returned by GetStatus endpoint
type GetConnectorStatusResponse struct {
	Code            int
	Name            string            `json:"name"`
	ConnectorStatus map[string]string `json:"connector"`
	TasksStatus     []TaskStatus      `json:"tasks"`
	ErrorResponse
}

//EmptyResponse is response returned by multiple endpoint when only StatusCode matter
type EmptyResponse struct {
	Code int
	ErrorResponse
}

//GetAll gets the list of all active connectors
func (c Client) GetAll() (GetAllConnectorsResponse, error) {
	resp := GetAllConnectorsResponse{}
	var connectors []string

	statusCode, err := c.Request(http.MethodGet, "connectors", nil, &connectors)
	if err != nil {
		return GetAllConnectorsResponse{}, err
	}

	resp.Code = statusCode
	resp.Connectors = connectors

	return resp, nil
}

//GetConnector return information on specific connector
func (c Client) GetConnector(req ConnectorRequest) (ConnectorResponse, error) {
	resp := ConnectorResponse{}

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s", req.Name), nil, &resp)
	if err != nil {
		return ConnectorResponse{}, err
	}

	// because connector missing is not an error
	if resp.ErrorCode != 0 && resp.ErrorCode != 404 {
		return ConnectorResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode
	return resp, nil
}

//CreateConnector create connector using specified config and name
func (c Client) CreateConnector(req CreateConnectorRequest, sync bool) (ConnectorResponse, error) {
	resp := ConnectorResponse{}

	statusCode, err := c.Request(http.MethodPost, "connectors", req, &resp)
	if err != nil {
		return ConnectorResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return ConnectorResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode

	if sync {
		if !TryUntil(
			func() bool {
				resp, err := c.GetConnector(req.ConnectorRequest)
				return err == nil && resp.Code == 200
			},
			2*time.Minute,
		) {
			return resp, errors.New("timeout on creating connector sync")
		}
	}

	return resp, nil
}

//UpdateConnector update a connector config
func (c Client) UpdateConnector(req CreateConnectorRequest, sync bool) (ConnectorResponse, error) {
	resp := ConnectorResponse{}

	statusCode, err := c.Request(http.MethodPut, fmt.Sprintf("connectors/%s/config", req.Name), req.Config, &resp)
	if err != nil {
		return ConnectorResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return ConnectorResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode

	if sync {
		if !TryUntil(
			func() bool {
				uptodate, err := c.IsUpToDate(req.Name, req.Config)
				return err == nil && uptodate
			},
			2*time.Minute,
		) {
			return resp, errors.New("timeout on creating connector sync")
		}
	}

	return resp, nil
}

//DeleteConnector delete a connector
func (c Client) DeleteConnector(req ConnectorRequest, sync bool) (EmptyResponse, error) {
	resp := EmptyResponse{}

	statusCode, err := c.Request(http.MethodDelete, fmt.Sprintf("connectors/%s", req.Name), nil, &resp)
	if err != nil {
		return EmptyResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return EmptyResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode

	if sync {
		if !TryUntil(
			func() bool {
				r, e := c.GetConnector(req)
				return e == nil && r.Code == 404
			},
			2*time.Minute,
		) {
			return resp, errors.New("timeout on deleting connector sync")
		}
	}

	return resp, nil
}

//GetConnectorConfig return config of a connector
func (c Client) GetConnectorConfig(req ConnectorRequest) (GetConnectorConfigResponse, error) {
	resp := GetConnectorConfigResponse{}
	var config map[string]string

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s/config", req.Name), nil, &config)
	if err != nil {
		return GetConnectorConfigResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return GetConnectorConfigResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode
	resp.Config = config
	return resp, nil
}

//GetConnectorStatus return current status of connector
func (c Client) GetConnectorStatus(req ConnectorRequest) (GetConnectorStatusResponse, error) {
	resp := GetConnectorStatusResponse{}

	statusCode, err := c.Request(http.MethodGet, fmt.Sprintf("connectors/%s/status", req.Name), nil, &resp)
	if err != nil {
		return GetConnectorStatusResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return GetConnectorStatusResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode
	return resp, nil
}

//RestartConnector restart connector
func (c Client) RestartConnector(req ConnectorRequest) (EmptyResponse, error) {
	resp := EmptyResponse{}

	statusCode, err := c.Request(http.MethodPost, fmt.Sprintf("connectors/%s/restart", req.Name), nil, &resp)
	if err != nil {
		return EmptyResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return EmptyResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode
	return resp, nil
}

//PauseConnector pause a running connector
//asynchronous operation
func (c Client) PauseConnector(req ConnectorRequest, sync bool) (EmptyResponse, error) {
	resp := EmptyResponse{}

	statusCode, err := c.Request(http.MethodPut, fmt.Sprintf("connectors/%s/pause", req.Name), nil, &resp)
	if err != nil {
		return EmptyResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return EmptyResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode

	if sync {
		if !TryUntil(
			func() bool {
				resp, err := c.GetConnectorStatus(req)
				return err == nil && resp.Code == 200 && resp.ConnectorStatus["state"] == "PAUSED"
			},
			2*time.Minute,
		) {
			return resp, errors.New("timeout on pausing connector sync")
		}
	}
	return resp, nil
}

//ResumeConnector resume a paused connector
//asynchronous operation
func (c Client) ResumeConnector(req ConnectorRequest, sync bool) (EmptyResponse, error) {
	resp := EmptyResponse{}

	statusCode, err := c.Request(http.MethodPut, fmt.Sprintf("connectors/%s/resume", req.Name), nil, &resp)
	if err != nil {
		return EmptyResponse{}, err
	}
	if resp.ErrorCode != 0 {
		return EmptyResponse{}, resp.ErrorResponse
	}

	resp.Code = statusCode

	if sync {
		if !TryUntil(
			func() bool {
				resp, err := c.GetConnectorStatus(req)
				return err == nil && resp.Code == 200 && resp.ConnectorStatus["state"] == "RUNNING"
			},
			2*time.Minute,
		) {
			return resp, errors.New("timeout on resuming connector sync")
		}
	}
	return resp, nil
}

func (c Client) IsUpToDate(connector string, config map[string]string) (bool, error) {
	config["name"] = connector

	configResp, err := c.GetConnectorConfig(ConnectorRequest{Name: connector})
	if err != nil {
		return false, err
	}
	if configResp.Code == 404 {
		return false, nil
	}
	if configResp.Code >= 400 {
		return false, errors.New(fmt.Sprintf("status code: %d", configResp.Code))
	}

	if len(configResp.Config) != len(config) {
		return false, nil
	}
	for key, value := range configResp.Config {
		if config[key] != value {
			return false, nil
		}
	}
	return true, nil
}

func TryUntil(exec func() bool, limit time.Duration) bool {
	timeLimit := time.After(limit)

	run := true
	defer func() { run = false }()
	success := make(chan bool)
	go func() {
		for run {
			if exec() {
				success <- true
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()

	select {
	case <-timeLimit:
		return false
	case <-success:
		return true
	}
}

func (c Client) DeployConnector(req CreateConnectorRequest) (err error) {
	existingConnector, err := c.GetConnector(ConnectorRequest{Name: req.Name})
	if err != nil {
		return err
	}

	if existingConnector.Code != 404 {
		var uptodate bool
		uptodate, err = c.IsUpToDate(req.Name, req.Config)
		if err != nil {
			return err
		}
		// Connector is already up to date, stop there and return ok
		if uptodate {
			return nil
		}

		_, err = c.PauseConnector(ConnectorRequest{Name: req.Name}, true)
		if err != nil {
			return err
		}

		defer func() {
			_, err = c.ResumeConnector(ConnectorRequest{Name: req.Name}, true)
		}()
	}

	_, err = c.UpdateConnector(req, true)
	if err != nil {
		return err
	}

	return err
}
