package connectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

//Client represents the kafka connect access configuration
type Client struct {
	URL string
}

//ErrorResponse is generic error returned by kafka connect
type ErrorResponse struct {
	ErrorCode int    `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

func (err ErrorResponse) Error() string {
	return fmt.Sprintf("error code: %d , message: %s", err.ErrorCode, err.Message)
}

//NewClient generates a new client
func NewClient(url string) Client {
	return Client{URL: url}
}

//Request handles an HTTP Get request to the client
// execute request and return parsed body content in result var
// result need to be pointer to expected type
func (c Client) Request(method string, endpoint string, request interface{}, result interface{}) (int, error) {

	endPointURL, err := url.Parse(c.URL + "/" + endpoint)
	if err != nil {
		return 0, err
	}

	buf := bytes.Buffer{}
	if request != nil {
		err = json.NewEncoder(&buf).Encode(request)
		if err != nil {
			return 0, err
		}
	}

	req, err := http.NewRequest(method, endPointURL.String(), bytes.NewReader(buf.Bytes()))
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return 0, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	if result != nil && res.Body != nil {
		b := bytes.Buffer{}
		b.ReadFrom(res.Body)
		if len(b.Bytes()) > 0 {
			err = json.NewDecoder(&b).Decode(result)
			if err != nil {
				return res.StatusCode, err
			}
		}
	}

	return res.StatusCode, nil
}
