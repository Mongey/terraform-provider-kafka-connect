package connect

import (
	"fmt"
	"testing"

	r "github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
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
				Config: testResourceConnector_updateConfig,
				Check:  testResourceConnector_updateCheck,
			},
			/*{
				Config: testResourceConnector_checkDoesNotLeakSensitiveConfig,
				Check: testResourceConnector_checkDoesNotLeakSensitive,
			},*/
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

	client := testProvider.Meta().(kc.Client)

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
	client := testProvider.Meta().(kc.Client)

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

func testResourceConnector_checkDoesNotLeakSensitive(s *terraform.State) error {
	fmt.Printf("[INFO] testing that sensitive fields do not leak into normal config.")
	client := testProvider.Meta().(kc.Client)

	c, err := client.GetConnector(kc.ConnectorRequest{Name: "sqlite-sink"})
	if err != nil {
		return err
	}

	if val, ok := c.Config["password"]; ok {
		return fmt.Errorf("password field from sensitive config has leaked into normal config! the password field there is %s", val)
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

const testResourceConnector_checkDoesNotLeakSensitiveConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = 1
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }

  config_sensitive = {
    "password" = "this-should-never-appear-unmasked"
  }
}
`
