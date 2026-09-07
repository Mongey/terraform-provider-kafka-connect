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

// ---------------------------------------------------------------------------
// Regression tests: Kafka Connect masks sensitive values (passwords) in every
// response it returns. Any config comparison that treats those masked values
// as ordinary values can never converge, which turns a successful update into
// a timeout. The tests below use a fake Kafka Connect REST endpoint so they
// run without TF_ACC and without docker-compose.
// ---------------------------------------------------------------------------

// maskedValue is what Kafka Connect returns instead of a secret: a run of
// U+2022 (BULLET) characters.
const maskedValue = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"

const testConnectorName = "sqlite-sink-secret"

// desiredConnectorConfig is what the practitioner declares in HCL: the real,
// plaintext password lives in config_sensitive.
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

// maskedRemoteConfig is what Kafka Connect answers with: identical to the
// desired config except the sensitive key, which comes back masked.
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

// newMaskingConnectAPI starts a minimal Kafka Connect REST API that always
// masks sensitive values, exactly like a real broker does.
func newMaskingConnectAPI(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		remote := maskedRemoteConfig()
		w.Header().Set("Content-Type", "application/json")

		switch {
		// GET/PUT /connectors/{name}/config
		case strings.HasSuffix(req.URL.Path, "/config"):
			_ = json.NewEncoder(w).Encode(remote)

		// GET /connectors/{name}
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

// TestMaskedPasswordIsUpToDate documents the root cause.
//
// Steps:
//  1. Start a fake Kafka Connect that masks "connection.password".
//  2. Build the desired config the provider would send (plaintext password).
//  3. Ask the go-kafka-connect client whether the connector is up to date.
//     This is the very check the library performs internally when the provider
//     calls CreateConnector/UpdateConnector with sync=true.
//  4. Expect true: the connector *is* up to date, the only difference is the
//     mask. A naive value-by-value comparison returns false forever, which is
//     what makes every apply hang until the sync timeout fires.
func TestMaskedPasswordIsUpToDate(t *testing.T) {
	srv := newMaskingConnectAPI(t)

	cfg, sensitive := desiredConnectorConfig()
	desired := combineMaps(cfg, sensitive)

	upToDate, err := kc.NewClient(srv.URL).IsUpToDate(testConnectorName, desired)
	if err != nil {
		t.Fatalf("unexpected error from IsUpToDate: %v", err)
	}
	if !upToDate {
		t.Fatalf("connector reported as NOT up to date only because Kafka Connect masked the " +
			"sensitive value; the config comparison must skip masked values, otherwise every " +
			"create/update with config_sensitive runs into a timeout")
	}
}

// TestConnectorUpdateWithMaskedSensitiveValue is the end-to-end regression
// test for the timeout itself.
//
// Steps:
//  1. Start the masking fake Kafka Connect.
//  2. Build ResourceData with config + config_sensitive, as Terraform would.
//  3. Run connectorUpdate against the fake broker in a goroutine.
//  4. Give it a hard 15s budget. The update does no real work, so it must
//     finish almost instantly. If the code waits for a masked value to compare
//     equal it will spin until the sync timeout (2 minutes in the library),
//     and this test fails with a clear message instead of hanging the suite.
//  5. Assert no error is returned - the update genuinely succeeded server-side.
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

// TestConnectorReadDoesNotLeakMaskedValue guards the other half of the
// contract: the mask must never end up in the non-sensitive "config" map,
// and the locally known plaintext secret must be preserved.
//
// Steps:
//  1. Start the masking fake Kafka Connect.
//  2. Build ResourceData holding the plaintext secret in config_sensitive.
//  3. Run connectorRead.
//  4. Assert "config" contains none of the sensitive keys and no bullet mask.
//  5. Assert "config_sensitive" still holds the original plaintext value,
//     so no permanent diff is produced on the next plan.
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
