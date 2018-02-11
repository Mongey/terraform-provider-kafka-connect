package kafka

import (
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
	name := d.Get("name").(string)

	cfg := d.Get("config").(map[string]interface{})
	config := make(map[string]string)

	for k, v := range cfg {
		switch v := v.(type) {
		case string:
			config[k] = v
		}
	}

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	_, err := c.CreateConnector(req, true)

	if err == nil {
		d.SetId(name)
	}

	return err
}

func connectorDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	req := kc.ConnectorRequest{
		Name: d.Get("name").(string),
	}
	_, err := c.DeleteConnector(req, true)

	if err == nil {
		d.SetId("")
	}

	return err
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := d.Get("name").(string)
	cfg := d.Get("config").(map[string]interface{})
	config := make(map[string]string)

	for k, v := range cfg {
		switch v := v.(type) {
		case string:
			config[k] = v
		}
	}

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	log.Printf("[INFO] Looking for %s", name)
	conn, err := c.UpdateConnector(req, true)

	if err == nil {
		log.Printf("[INFO] this the shit %v", conn.Config)
		d.Set("config", conn.Config)
	}

	return err
}
func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := d.Get("name").(string)
	req := kc.ConnectorRequest{
		Name: name,
	}
	log.Printf("[INFO] Looking for %s", name)
	conn, err := c.GetConnector(req)

	if err == nil {
		log.Printf("[INFO] this the shit %v", conn.Config)
		d.Set("config", conn.Config)
	}

	return err
}
