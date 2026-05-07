package connect

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
)

var testAccProvider *schema.Provider
var testAccProviderFactories map[string]func() (*schema.Provider, error)

func init() {
	testAccProvider = Provider()
	testAccProviderFactories = map[string]func() (*schema.Provider, error){
		"kafka-connect": func() (*schema.Provider, error) {
			return testAccProvider, nil
		},
	}
}

func TestAccConnectorStatus_lifecycle(t *testing.T) {
	var connectorName = "test-connector-lifecycle"

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccConnectorCheckDestroy,
		Steps: []resource.TestStep{
			// Before starting, we need a connector to exist. This step is assumed to create it.
			// In a real scenario, you would have a kafka-connect_connector resource here.
			{
				Config: testAccConnectorConfig_basic(connectorName),
			},
			// Step 1: Create status resource for the existing connector, set to RUNNING
			{
				Config: testAccConnectorStatusConfig(connectorName, ConnectorStatusRunning),
				Check: resource.ComposeTestCheckFunc(
					testAccConnectorStatusExists("kafka-connect_connector_status.test", ConnectorStatusRunning),
					resource.TestCheckResourceAttr("kafka-connect_connector_status.test", "state", ConnectorStatusRunning),
				),
			},
			// Step 2: Update status to PAUSED
			{
				Config: testAccConnectorStatusConfig(connectorName, ConnectorStatusPaused),
				Check: resource.ComposeTestCheckFunc(
					testAccConnectorStatusExists("kafka-connect_connector_status.test", ConnectorStatusPaused),
					resource.TestCheckResourceAttr("kafka-connect_connector_status.test", "state", ConnectorStatusPaused),
				),
			},
			// Step 3: Update status back to RUNNING
			{
				Config: testAccConnectorStatusConfig(connectorName, ConnectorStatusRunning),
				Check: resource.ComposeTestCheckFunc(
					testAccConnectorStatusExists("kafka-connect_connector_status.test", ConnectorStatusRunning),
					resource.TestCheckResourceAttr("kafka-connect_connector_status.test", "state", ConnectorStatusRunning),
				),
			},
		},
	})
}

func testAccConnectorStatusExists(resourceName string, expectedStatus string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set for %s", resourceName)
		}

		client := testAccProvider.Meta().(kc.HighLevelClient)
		connectorName := rs.Primary.ID

		req := kc.ConnectorRequest{Name: connectorName}
		status, err := client.GetConnectorStatus(req)
		if err != nil {
			return fmt.Errorf("error getting connector status for %s: %s", connectorName, err)
		}

		// The resource handles 404 by clearing the ID, but in an Exists check, a 404 is an error.
		if status.Code == 404 || status.ConnectorStatus == nil {
			return fmt.Errorf("connector '%s' not found on API", connectorName)
		}

		actualStatus := status.ConnectorStatus["state"]

		// The resource treats UNASSIGNED as RUNNING, so we need to account for that in the check.
		if expectedStatus == ConnectorStatusRunning && actualStatus == ConnectorStatusUnassigned {
			return nil // This is an acceptable transient state.
		}

		if actualStatus != expectedStatus {
			return fmt.Errorf("expected connector status to be '%s', but got '%s'", expectedStatus, actualStatus)
		}

		return nil
	}
}

func testAccConnectorCheckDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "kafka-connect_connector" {
			continue
		}

		client := testAccProvider.Meta().(kc.HighLevelClient)
		connectorName := rs.Primary.ID

		req := kc.ConnectorRequest{Name: connectorName}
		status, err := client.GetConnectorStatus(req)
		if err != nil {
			return fmt.Errorf("error getting connector status for %s: %s", connectorName, err)
		}

		// If it is found (not 404), is an error.
		if status.Code != 404 {
			return fmt.Errorf("connector '%s' still there after destroy", connectorName)
		}
	}
	return nil
}

// Generates HCL for a kafka-connect_connector resource, needed as a prerequisite.
func testAccConnectorConfig_basic(connectorName string) string {
	return fmt.Sprintf(`
resource "kafka-connect_connector" "test" {
  name = "%s"

  config = {
    "name"            = "%s"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "2"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`, connectorName, connectorName)
}

// Generates HCL for the kafka-connect_connector_status resource.
func testAccConnectorStatusConfig(connectorName string, state string) string {
	// We depend on the connector existing first.
	return testAccConnectorConfig_basic(connectorName) + fmt.Sprintf(`
resource "kafka-connect_connector_status" "test" {
  name  = kafka-connect_connector.test.name
  state = "%s"
}
`, state)
}
