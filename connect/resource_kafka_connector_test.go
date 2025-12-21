package connect

import (
	"errors"
	"fmt"
	"testing"
	"time"

	r "github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
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
