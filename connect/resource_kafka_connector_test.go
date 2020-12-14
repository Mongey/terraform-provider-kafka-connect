package connect

import (
	"fmt"
	"testing"

	r "github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	kc "github.com/ricardo-ch/go-kafka-connect/lib/connectors"
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
