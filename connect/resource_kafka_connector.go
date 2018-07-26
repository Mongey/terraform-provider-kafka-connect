package connect

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	kc "github.com/ricardo-ch/go-kafka-connect/lib/connectors"
)

func kafkaConnectorResource() *schema.Resource {
	return &schema.Resource{
		Create: connectorCreate,
		Read:   connectorRead,
		Update: connectorUpdate,
		Delete: connectorDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
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
				Description: "A map of string k/v attributes",
			},
		},
	}
}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)
	name := nameFromRD(d)
	config := configFromRD(d)
	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	connectorResponse, err := c.CreateConnector(req, true)
	if err != nil {
		fmt.Printf(err)
		return err
	}
	fmt.Printf("[INFO] Created the connector %v\n", connectorResponse)
	d.SetId(name)
	return nil
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
}

func connectorDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := nameFromRD(d)
	req := kc.ConnectorRequest{
		Name: name,
	}

	fmt.Printf("[INFO] Deleting the connector %s\n", name)
	_, err := c.DeleteConnector(req, true)
	if err != nil {
		fmt.Printf(err)
		return err
	}
	fmt.Printf("[INFO] Connector %s deleted\n", name)
	d.SetId("")
	return nil
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := nameFromRD(d)
	config := configFromRD(d)

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	log.Printf("[INFO] Looking for %s", name)
	conn, err := c.UpdateConnector(req, true)
	if err != nil {
		fmt.Printf(err)
		return err
	}
	log.Printf("[INFO] Config updated %v", conn.Config)
	d.Set("config", conn.Config)
	return nil
}

func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := d.Get("name").(string)
	req := kc.ConnectorRequest{
		Name: name,
	}
	log.Printf("[INFO] Looking for %s", name)
	conn, err := c.GetConnector(req)
	if err != nil {
		fmt.Printf(err)
		return err
	}

	log.Printf("[INFO] found the config %v", conn.Config)
	d.Set("config", conn.Config)
	return nil
}

func configFromRD(d *schema.ResourceData) map[string]string {
	cfg := d.Get("config").(map[string]interface{})
	config := make(map[string]string)

	for k, v := range cfg {
		switch v := v.(type) {
		case string:
			config[k.(string)] = v.(string)
		}
	}

	return config
}
