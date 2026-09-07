package connect

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
)

func kafkaConnectorResource() *schema.Resource {
	return &schema.Resource{
		Create: connectorCreate,
		Read:   connectorRead,
		Update: connectorUpdate,
		Delete: connectorDelete,
		Importer: &schema.ResourceImporter{
			State: setNameFromID,
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Second),
			Update: schema.DefaultTimeout(60 * time.Second),
			Delete: schema.DefaultTimeout(60 * time.Second),
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the connector",
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
			},
			"config_sensitive": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Sensitive:   true,
				Description: "A map of string k/v attributes which are sensitive, such as passwords.",
			},
		},
	}
}

func setNameFromID(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {

	connectorName := d.Id()
	log.Printf("Import connector with name: %s", connectorName)
	d.Set("name", connectorName)

	return []*schema.ResourceData{d}, nil
}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)
	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)
	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is the mandatory field identical to the resource name")
	}

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	// Use retry logic for CreateConnector to handle race conditions
	// where connector is created but GetConnector returns 409 if called too quickly.
	// The library-side sync wait (sync=true) is deliberately disabled: it compares
	// the config value by value and never converges once Kafka Connect masks a secret.
	var connectorResponse kc.ConnectorResponse
	err := withRebalanceRetry(func() error {
		var createErr error
		connectorResponse, createErr = c.CreateConnector(req, false)
		return createErr
	}, d.Timeout(schema.TimeoutCreate))

	fmt.Printf("[INFO] Created the connector %v\n", connectorResponse)

	if err == nil {
		newConfFiltered := removeSecondKeysFromFirst(connectorResponse.Config, sensitiveCache)
		d.SetId(name)
		d.Set("config_sensitive", sensitiveCache)
		d.Set("config", newConfFiltered)
	}

	if err != nil {
		return err
	}

	if err := waitForConnectorAvailable(c, name, d.Timeout(schema.TimeoutCreate)); err != nil {
		return err
	}

	return readWithRetry(d, meta, d.Timeout(schema.TimeoutCreate))
}

func connectorDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)
	req := kc.ConnectorRequest{
		Name: name,
	}

	fmt.Printf("[INFO] Deleting the connector %s\n", name)

	err := withRebalanceRetry(func() error {
		_, derr := c.DeleteConnector(req, false)
		return derr
	}, d.Timeout(schema.TimeoutDelete))
	if err != nil {
		return err
	}

	if err := waitForConnectorGone(c, name, d.Timeout(schema.TimeoutDelete)); err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)
	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is the mandatory field identical to the resource name")
	}

	log.Printf("[INFO] Requesting update to connector %v", name)
	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	log.Printf("[INFO] Looking for %s", name)
	var conn kc.ConnectorResponse
	var err error
	err = withRebalanceRetry(func() error {
		conn, err = c.UpdateConnector(req, false)
		return err
	}, d.Timeout(schema.TimeoutUpdate))

	if err == nil {
		newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)
		//log.Printf("[INFO] Full config received from update is: %v", conn.Config)
		log.Printf("[INFO] Local config nonsensitive updated to: %v", newConfFiltered)
		//log.Printf("[INFO] Local config_sensitive updated to:  %v", sensitiveCache)
		d.Set("config", newConfFiltered)
		d.Set("config_sensitive", sensitiveCache)
	}

	if err != nil {
		return err
	}

	if err := waitForConnectorUpToDate(c, name, config, d.Timeout(schema.TimeoutUpdate)); err != nil {
		return err
	}

	return readWithRetry(d, meta, d.Timeout(schema.TimeoutUpdate))
}

func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	config, sensitiveCache := configFromRD(d)
	name := d.Get("name").(string)
	req := kc.ConnectorRequest{
		Name: name,
	}

	log.Printf("[INFO] Attempting to read remote data for connector %s", name)
	log.Printf("[INFO] Current local config nonsensitive values are: %v", config)
	//log.Printf("[INFO] Current local config_sensitive values are: %v", sensitiveCache)
	conn, err := c.GetConnector(req)

	if err != nil {
		return err
	}

	// we do not want the sensitive values to appear in the non-masked 'config' field
	// use cached sensitive values to get the correct keys to remove from the newly read config
	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)
	d.Set("config_sensitive", sensitiveCache)
	d.Set("config", newConfFiltered)
	log.Printf("[INFO] Local config nonsensitive data updated to %v", newConfFiltered)
	//log.Printf("[INFO] Local config_sensitive data updated to %v", sensitiveCache)

	return nil
}

// readWithRetry wraps the connectorRead function with retry functionality that
// will attempt to read the connector again if a rebalance operation is
// detected.
func readWithRetry(d *schema.ResourceData, meta interface{}, timeout time.Duration) error {
	return withRebalanceRetry(func() error {
		return connectorRead(d, meta)
	}, timeout)
}

// waitForConnectorAvailable polls until the connector responds with HTTP 200 or timeout is reached.
func waitForConnectorAvailable(c kc.HighLevelClient, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	req := kc.ConnectorRequest{Name: name}
	for {
		resp, err := c.GetConnector(req)
		if err == nil && resp.Code == 200 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for connector %q to become available", name)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitForConnectorGone polls until the connector is no longer found (HTTP 404) or timeout is reached.
func waitForConnectorGone(c kc.HighLevelClient, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	req := kc.ConnectorRequest{Name: name}
	for {
		resp, err := c.GetConnector(req)
		if err == nil && resp.Code == 404 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for connector %q to be deleted", name)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitForConnectorUpToDate polls until the running connector config matches the desired state or timeout is reached.
// It uses a custom comparison that skips values masked by Kafka Connect (e.g. password fields returned as "••••••••••••").
func waitForConnectorUpToDate(c kc.HighLevelClient, name string, config map[string]interface{}, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	req := kc.ConnectorRequest{Name: name}
	for {
		configResp, err := c.GetConnectorConfig(req)
		if err == nil && configResp.Code == 200 && isConfigUpToDate(config, configResp.Config) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for connector %q config to be applied", name)
		}
		time.Sleep(2 * time.Second)
	}
}

// isConfigUpToDate compares desired config with the remote config, skipping
// values that Kafka Connect has masked (e.g. "••••••••••••" for passwords).
func isConfigUpToDate(desired map[string]interface{}, remote map[string]interface{}) bool {
	for key, desiredVal := range desired {
		remoteVal, ok := remote[key]
		if !ok {
			return false
		}
		remoteStr := fmt.Sprintf("%v", remoteVal)
		if isMaskedValue(remoteStr) {
			continue
		}
		if fmt.Sprintf("%v", desiredVal) != remoteStr {
			return false
		}
	}
	return true
}

// isMaskedValue returns true if the value looks like a Kafka Connect masked secret.
func isMaskedValue(v string) bool {
	for _, r := range v {
		if r != '\u2022' {
			return false
		}
	}
	return len(v) > 0
}

// withRebalanceRetry executes the provided function with exponential backoff
// retry logic specifically designed to handle Kafka Connect rebalancing
// scenarios. The timeout parameter specifies how long to wait for rebalancing
// to complete.
func withRebalanceRetry(fn func() error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 250 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRebalanceError(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Kafka Connect rebalance to finish: %w", err)
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		sleep := backoff + jitter
		log.Printf("[INFO] Connect rebalance in progress; retrying after %.2fs ... (%v)", sleep.Seconds(), err)
		time.Sleep(sleep)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// isRebalanceError tries to detect the Connect 409 window and related exceptions.
func isRebalanceError(err error) bool {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "rebalance") ||
		strings.Contains(msg, "rebalanceexpected") ||
		strings.Contains(msg, "rebalance is expected") ||
		strings.Contains(msg, "conflicting operation") {
		return true
	}

	return strings.Contains(msg, "409")
}

// Returns a full config (inclusive of sensitive values) and a config of just the sensitive values
// The first is intended to be passed to CreateConnectorRequest
// The second is intended to preserve knowledge of which keys are sensitive information in the incoming
// ConnectorResponse.Config
func configFromRD(d *schema.ResourceData) (map[string]interface{}, map[string]interface{}) {
	cfg := mapFromRD(d, "config")
	scfg := mapFromRD(d, "config_sensitive")
	config := combineMaps(cfg, scfg)
	return config, scfg
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
}

func mapFromRD(d *schema.ResourceData, key string) map[string]interface{} {
	return d.Get(key).(map[string]interface{})
}

// if there are duplicate keys this will always take the kv from second!!!
func combineMaps(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	union := make(map[string]interface{})
	for k, v := range first {
		union[k] = v
	}
	for k, v := range second {
		union[k] = v
	}
	return union
}

func removeSecondKeysFromFirst(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	for k := range second {
		delete(first, k)
	}
	return first
}
