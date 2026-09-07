package connect

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	r "github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
)

func TestAccConnectorConfigUpdate(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnector_initialConfig,
				Check:  testResourceConnector_initialCheck,
			},
			{
				Config:            testResourceConnector_initialConfig,
				ResourceName:      "kafka-connect_connector.test",
				ImportStateVerify: true,
				ImportState:       true,
			},
			{
				Config: testResourceConnector_updateConfig,
				Check:  testResourceConnector_updateCheck,
			},
		},
	})
}

// TestAccConnectorWithTimeouts tests that custom timeouts are properly accepted
func TestAccConnectorWithTimeouts(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnector_withTimeouts,
				Check: r.ComposeTestCheckFunc(
					r.TestCheckResourceAttr("kafka-connect_connector.test_timeouts", "name", "test-with-timeouts"),
					r.TestCheckResourceAttr("kafka-connect_connector.test_timeouts", "config.tasks.max", "1"),
					// Verify the connector was created (if it has an ID, it was created successfully)
					r.TestCheckResourceAttrSet("kafka-connect_connector.test_timeouts", "id"),
				),
			},
		},
	})
}

func testResourceConnector_initialCheck(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka-connect_connector.test"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	name := instanceState.ID

	if name != instanceState.Attributes["name"] {
		return fmt.Errorf("id doesn't match name")
	}

	client := testProvider.Meta().(kc.HighLevelClient)

	c, err := client.GetConnector(kc.ConnectorRequest{Name: "sqlite-sink"})
	if err != nil {
		return err
	}

	tasksMax := c.Config["tasks.max"]
	expected := "2"
	if tasksMax != expected {
		return fmt.Errorf("tasks.max should be %s, got %s connector not updated. \n %v", expected, tasksMax, c.Config)
	}

	return nil
}

func testResourceConnector_updateCheck(s *terraform.State) error {
	client := testProvider.Meta().(kc.HighLevelClient)

	c, err := client.GetConnector(kc.ConnectorRequest{Name: "sqlite-sink"})
	if err != nil {
		return err
	}

	tasksMax := c.Config["tasks.max"]
	expected := "1"
	if tasksMax != expected {
		return fmt.Errorf("tasks.max should be %s, got %s connector not updated. \n %v", expected, tasksMax, c.Config)
	}

	return nil
}

const testResourceConnector_initialConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
		"name" = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "2"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

const testResourceConnector_updateConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
		"name" = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

// Test configuration with custom timeouts
const testResourceConnector_withTimeouts = `
resource "kafka-connect_connector" "test_timeouts" {
  name = "test-with-timeouts"

  config = {
    "name"            = "test-with-timeouts"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "test-topic"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }

  timeouts {
    create = "10m"
    update = "8m"
    delete = "3m"
  }
}
`

func TestIsRebalanceError(t *testing.T) {
	rebalanceErr := errors.New("rebalance in progress")
	if !isRebalanceError(rebalanceErr) {
		t.Errorf("expected rebalance error to be detected")
	}

	normalErr := errors.New("connection timeout")
	if isRebalanceError(normalErr) {
		t.Errorf("expected normal error to not be detected as rebalance error")
	}
}

func TestWithRebalanceRetry(t *testing.T) {
	t.Run("successful operation after rebalance errors", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			if callCount < 3 {
				return errors.New("rebalance in progress")
			}
			return nil
		}

		err := withRebalanceRetry(operation, 5*time.Second)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
		if callCount != 3 {
			t.Errorf("expected 3 calls, got %d", callCount)
		}
	})

	t.Run("non-rebalance error should not retry", func(t *testing.T) {
		callCount := 0
		expectedErr := errors.New("connection timeout")
		operation := func() error {
			callCount++
			return expectedErr
		}

		err := withRebalanceRetry(operation, 5*time.Second)
		if err != expectedErr {
			t.Errorf("expected error %v, got: %v", expectedErr, err)
		}
		if callCount != 1 {
			t.Errorf("expected 1 call, got %d", callCount)
		}
	})

	t.Run("timeout should be respected", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			// Always return rebalance error to force timeout
			return errors.New("rebalance in progress")
		}

		start := time.Now()
		err := withRebalanceRetry(operation, 1*time.Second)
		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}
		if !errors.Is(err, errors.New("rebalance in progress")) {
			if err.Error() != "timed out waiting for Kafka Connect rebalance to finish: rebalance in progress" {
				t.Errorf("expected timeout error message, got: %v", err)
			}
		}
		// Verify timeout was respected (should be around 1s, allow generous margin for backoff and jitter)
		if duration < 800*time.Millisecond || duration > 2500*time.Millisecond {
			t.Errorf("expected timeout around 1s (with margin for backoff), got %v", duration)
		}
		if callCount < 2 {
			t.Errorf("expected at least 2 retry attempts, got %d", callCount)
		}
	})

	t.Run("long timeout allows many retries", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			if callCount < 5 {
				return errors.New("rebalance in progress")
			}
			return nil
		}

		err := withRebalanceRetry(operation, 30*time.Second)
		if err != nil {
			t.Errorf("expected no error with long timeout, got: %v", err)
		}
		if callCount != 5 {
			t.Errorf("expected 5 calls, got %d", callCount)
		}
	})

	t.Run("very short timeout fails quickly", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			// Add small delay to ensure timeout is hit
			time.Sleep(100 * time.Millisecond)
			return errors.New("rebalance in progress")
		}

		start := time.Now()
		err := withRebalanceRetry(operation, 50*time.Millisecond)
		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}
		// With 50ms timeout and 100ms operation, should fail on first attempt
		if duration > 200*time.Millisecond {
			t.Errorf("expected fast timeout, got %v", duration)
		}
		if callCount != 1 {
			t.Errorf("expected 1 call with very short timeout, got %d", callCount)
		}
	})
}

// Regression tests for masked sensitive values returned by Kafka Connect.

// maskedValue is a common Kafka Connect representation of a secret.
const maskedValue = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"

const testConnectorName = "sqlite-sink-secret"

// desiredConnectorConfig returns a representative connector configuration.
func desiredConnectorConfig() (nonSensitive map[string]interface{}, sensitive map[string]interface{}) {
	return map[string]interface{}{
		"name":            testConnectorName,
		"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
		"tasks.max":       "1",
		"topics":          "orders",
		"connection.url":  "jdbc:sqlite:test.db",
	}, map[string]interface{}{
		"connection.password": "s3cr3t-plaintext",
	}
}

// maskedRemoteConfig returns the desired config with its secret masked.
func maskedRemoteConfig() map[string]interface{} {
	cfg, sensitive := desiredConnectorConfig()
	remote := make(map[string]interface{}, len(cfg)+len(sensitive))
	for k, v := range cfg {
		remote[k] = v
	}
	for k := range sensitive {
		remote[k] = maskedValue
	}
	return remote
}

// newMaskingConnectAPI starts a minimal API that returns masked secrets.
func newMaskingConnectAPI(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		remote := maskedRemoteConfig()
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.HasSuffix(req.URL.Path, "/config"):
			_ = json.NewEncoder(w).Encode(remote)

		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"name":   testConnectorName,
				"config": remote,
				"tasks":  []interface{}{},
			})
		}
	}))
	t.Cleanup(srv.Close)

	return srv
}

// TestConfigDiffWithMaskedValues verifies masked secrets do not cause a diff.
func TestConfigDiffWithMaskedValues(t *testing.T) {
	cfg, sensitive := desiredConnectorConfig()
	desired := combineMaps(cfg, sensitive)

	if diff := configDiff(desired, maskedRemoteConfig(), sensitive); len(diff) != 0 {
		t.Errorf("config reported as NOT up to date only because Kafka Connect masked the "+
			"sensitive value; masked values must be skipped, otherwise every create/update "+
			"with config_sensitive runs into a timeout (diff: %v)", diff)
	}

	stale := maskedRemoteConfig()
	stale["tasks.max"] = "99"
	diff := configDiff(desired, stale, sensitive)
	if len(diff) != 1 || !strings.HasPrefix(diff[0], "tasks.max ") {
		t.Errorf("a real difference in tasks.max must be reported by key name, got %v", diff)
	}
}

// TestConfigDiffConvergesForEveryMaskFormat covers supported mask formats.
func TestConfigDiffConvergesForEveryMaskFormat(t *testing.T) {
	const mTLSKey = "producer.override.ssl.keystore.password"

	cfg, _ := desiredConnectorConfig()
	sensitive := map[string]interface{}{mTLSKey: "keystore-plaintext"}
	desired := combineMaps(cfg, sensitive)

	masks := map[string]string{
		"bullets":         maskedValue,
		"kafka [hidden]":  "[hidden]",
		"unknown format":  "<redacted-by-broker>",
		"empty on broker": "",
	}

	for name, mask := range masks {
		t.Run(name, func(t *testing.T) {
			remote := combineMaps(cfg, map[string]interface{}{mTLSKey: mask})
			if diff := configDiff(desired, remote, sensitive); len(diff) != 0 {
				t.Errorf("mask format %q broke convergence for a key declared in "+
					"config_sensitive: %v", mask, diff)
			}
		})
	}
}

// TestConfigDiffSkipsMaskedValueOutsideConfigSensitive covers the fallback for
// masked values that were not declared in config_sensitive.
func TestConfigDiffSkipsMaskedValueOutsideConfigSensitive(t *testing.T) {
	const mTLSKey = "producer.override.ssl.keystore.password"

	cfg, _ := desiredConnectorConfig()
	desired := combineMaps(cfg, map[string]interface{}{mTLSKey: "keystore-plaintext"})
	remote := combineMaps(cfg, map[string]interface{}{mTLSKey: maskedValue})

	if diff := configDiff(desired, remote, map[string]interface{}{}); len(diff) != 0 {
		t.Errorf("a masked value in the plain config block must not block convergence: %v", diff)
	}
}

// TestConfigDiffReportsMissingSensitiveKey ensures missing keys are reported.
func TestConfigDiffReportsMissingSensitiveKey(t *testing.T) {
	const typoKey = "producer.override.ssl.keystor.password"

	cfg, _ := desiredConnectorConfig()
	sensitive := map[string]interface{}{typoKey: "keystore-plaintext"}
	desired := combineMaps(cfg, sensitive)

	diff := configDiff(desired, cfg, sensitive)
	if len(diff) != 1 || !strings.Contains(diff[0], typoKey) || !strings.Contains(diff[0], "missing") {
		t.Errorf("a sensitive key absent from the connector must be reported, got %v", diff)
	}
}

// TestDescribeValueNeverLeaksTheValue guards diagnostic output.
func TestDescribeValueNeverLeaksTheValue(t *testing.T) {
	const secret = "s3cr3t-plaintext"

	got := describeValue(secret)
	if strings.Contains(got, secret) {
		t.Fatalf("describeValue leaked the raw value: %q", got)
	}
	if !strings.Contains(got, "16 chars") {
		t.Errorf("describeValue should report the length, got %q", got)
	}
}

// TestIsMaskedValue covers recognized and ordinary values.
func TestIsMaskedValue(t *testing.T) {
	masked := []string{maskedValue, "\u2022", "\u2022\u2022\u2022", "[hidden]"}
	for _, v := range masked {
		if !isMaskedValue(v) {
			t.Errorf("expected %q to be detected as masked", v)
		}
	}

	plain := []string{"", "s3cr3t", "1", "\u2022abc", "abc\u2022", "*", "***", ".*", "[HIDDEN]"}
	for _, v := range plain {
		if isMaskedValue(v) {
			t.Errorf("expected %q NOT to be detected as masked", v)
		}
	}
}

// TestConnectorUpdateWithMaskedSensitiveValue covers the update path.
func TestConnectorUpdateWithMaskedSensitiveValue(t *testing.T) {
	srv := newMaskingConnectAPI(t)

	cfg, sensitive := desiredConnectorConfig()
	d := schema.TestResourceDataRaw(t, kafkaConnectorResource().Schema, map[string]interface{}{
		"name":             testConnectorName,
		"config":           cfg,
		"config_sensitive": sensitive,
	})
	d.SetId(testConnectorName)

	done := make(chan error, 1)
	go func() {
		done <- connectorUpdate(d, kc.NewClient(srv.URL))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("update of a connector with a masked sensitive value failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("connectorUpdate did not return within 15s: it is waiting for the masked "+
			"value %q to become equal to the plaintext password, which never happens", maskedValue)
	}
}

// TestConnectorReadDoesNotLeakMaskedValue covers state handling after a read.
func TestConnectorReadDoesNotLeakMaskedValue(t *testing.T) {
	srv := newMaskingConnectAPI(t)

	cfg, sensitive := desiredConnectorConfig()
	d := schema.TestResourceDataRaw(t, kafkaConnectorResource().Schema, map[string]interface{}{
		"name":             testConnectorName,
		"config":           cfg,
		"config_sensitive": sensitive,
	})
	d.SetId(testConnectorName)

	if err := connectorRead(d, kc.NewClient(srv.URL)); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	readConfig := d.Get("config").(map[string]interface{})
	for key := range sensitive {
		if _, present := readConfig[key]; present {
			t.Errorf("sensitive key %q leaked into the non-sensitive config map", key)
		}
	}
	for key, value := range readConfig {
		if strings.ContainsRune(fmt.Sprintf("%v", value), '\u2022') {
			t.Errorf("masked value stored in config[%q] = %v", key, value)
		}
	}

	readSensitive := d.Get("config_sensitive").(map[string]interface{})
	for key, want := range sensitive {
		if got := fmt.Sprintf("%v", readSensitive[key]); got != fmt.Sprintf("%v", want) {
			t.Errorf("config_sensitive[%q] = %q, want the locally known plaintext value", key, got)
		}
	}
}

// TestWaitForConnectorUpToDateTimeoutNamesTheKeys verifies timeout diagnostics.
func TestWaitForConnectorUpToDateTimeoutNamesTheKeys(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		stale := maskedRemoteConfig()
		stale["tasks.max"] = "99"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stale)
	}))
	defer srv.Close()

	cfg, sensitive := desiredConnectorConfig()
	desired := combineMaps(cfg, sensitive)

	err := waitForConnectorUpToDate(kc.NewClient(srv.URL), testConnectorName, desired, sensitive, time.Second)
	if err == nil {
		t.Fatalf("expected a timeout error for a connector that never converges")
	}
	if !strings.Contains(err.Error(), "tasks.max") {
		t.Errorf("timeout error must name the keys that differ, got: %v", err)
	}
	for key := range sensitive {
		if strings.Contains(err.Error(), fmt.Sprintf("%v", sensitive[key])) {
			t.Errorf("timeout error leaked a sensitive value: %v", err)
		}
	}
}
