package connect

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"
	"unicode"

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

	// Use provider-side polling because Kafka Connect masks sensitive config values.
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

	if err := waitForConnectorUpToDate(c, name, config, sensitiveCache, d.Timeout(schema.TimeoutUpdate)); err != nil {
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

// waitForConnectorAvailable waits until the connector can be read.
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

// waitForConnectorGone waits until the connector is no longer found.
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

// waitForConnectorUpToDate waits until the remote config matches the desired state.
func waitForConnectorUpToDate(c kc.HighLevelClient, name string, config map[string]interface{}, sensitive map[string]interface{}, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	req := kc.ConnectorRequest{Name: name}
	for {
		configResp, err := c.GetConnectorConfig(req)
		if err == nil && configResp.Code == 200 {
			diff := configDiff(config, configResp.Config, sensitive)
			if len(diff) == 0 {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for connector %q config to be applied; keys still differing: %s",
					name, strings.Join(diff, ", "))
			}
			log.Printf("[DEBUG] Connector %q not converged yet, keys differing: %s", name, strings.Join(diff, ", "))
		} else if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for connector %q config to be applied: config could not be read (code %d, err %v)",
				name, configResp.Code, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// configDiff returns the desired keys that differ from the remote config.
// Sensitive values are not compared because Kafka Connect masks them, but their
// presence is still required.
func configDiff(desired map[string]interface{}, remote map[string]interface{}, sensitive map[string]interface{}) []string {
	var diff []string

	for key, desiredVal := range desired {
		remoteVal, present := remote[key]
		if !present {
			diff = append(diff, fmt.Sprintf("%s (missing on the connector)", key))
			continue
		}

		remoteStr := fmt.Sprintf("%v", remoteVal)

		if _, isSensitive := sensitive[key]; isSensitive {
			// The broker never returns the real value, so there is nothing to compare.
			continue
		}
		if isMaskedValue(remoteStr) {
			// Sensitive value declared in the plain `config` block; the broker masked it.
			log.Printf("[WARN] Connector config key %q is masked by Kafka Connect but is not "+
				"declared in config_sensitive; move it there to keep the secret out of the plain config", key)
			continue
		}
		if fmt.Sprintf("%v", desiredVal) != remoteStr {
			diff = append(diff, fmt.Sprintf("%s (remote: %s)", key, describeValue(remoteStr)))
		}
	}

	sort.Strings(diff)

	return diff
}

// describeValue provides diagnostics without printing the remote value.
func describeValue(v string) string {
	if v == "" {
		return "empty"
	}

	sum := sha256.Sum256([]byte(v))
	kind := "ascii"
	for _, r := range v {
		if r > unicode.MaxASCII {
			kind = "non-ascii"
			break
		}
	}

	return fmt.Sprintf("%d chars, %s, sha256=%x", len([]rune(v)), kind, sum[:4])
}

// isMaskedValue detects the mask formats commonly returned by Kafka Connect.
func isMaskedValue(v string) bool {
	if v == "" {
		return false
	}
	if v == "[hidden]" {
		return true
	}

	for _, r := range v {
		if r != '\u2022' {
			return false
		}
	}

	return true
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
