package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/phoops/ngsiv2/model"
)

type NgsiV2Client struct {
	c       *http.Client
	url     string
	timeout time.Duration
}

// ClientOptionFunc is a function that configures a NgsiV2Client.
type ClientOptionFunc func(*NgsiV2Client) error

// NewNgsiV2Client creates a new NGSIv2 client.
func NewNgsiV2Client(options ...ClientOptionFunc) (*NgsiV2Client, error) {
	c := &NgsiV2Client{
		timeout: time.Second * 15,
	}

	// apply the options
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	c.c = &http.Client{
		Timeout: c.timeout,
	}

	return c, nil
}

// SetClientTimeout is used to specify a value for http client timeout.
func SetClientTimeout(timeout time.Duration) ClientOptionFunc {
	return func(c *NgsiV2Client) error {
		c.timeout = timeout
		return nil
	}
}

func SetUrl(url string) ClientOptionFunc {
	return func(c *NgsiV2Client) error {
		c.url = url
		return nil
	}
}

func newRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "ngsiv2-client")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	return req, nil
}

func (c *NgsiV2Client) BatchUpdate(msg *model.BatchUpdate) error {
	jsonValue, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Could not serialize message: %+v", err)
	}
	req, err := newRequest("POST", fmt.Sprintf("%s/v2/op/update", c.url), bytes.NewBuffer(jsonValue))
	if err != nil {
		return fmt.Errorf("Could not create request for batch update: %+v", err)
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("Error invoking batch update: %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// RetrieveAPIResources gives url link values for retrieving resources.
// See: https://orioncontextbroker.docs.apiary.io/#reference/api-entry-point/retrieve-api-resources/retrieve-api-resources
func (c *NgsiV2Client) RetrieveAPIResources() (*model.APIResources, error) {
	req, err := newRequest("GET", fmt.Sprintf("%s/v2", c.url), nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for API resources: %+v", err)
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve API resources: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		ret := new(model.APIResources)
		if err := json.Unmarshal(bodyBytes, ret); err != nil {
			return nil, fmt.Errorf("Error reading API resources response: %+v", err)
		} else {
			return ret, nil
		}
	}
}
