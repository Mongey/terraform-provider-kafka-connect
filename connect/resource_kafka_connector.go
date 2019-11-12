package connect

import (
	"fmt"
	"log"
	"time"

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
				Description: "A map of string k/v attributes.",
			},
			"config_sensitive": {
				Type: schema.TypeMap,
				Optional: true,
				ForceNew: false,
				Sensitive: true,
				Description: "A map of string k/v attributes which are sensitive, such as passwords.",
			},
		},
	}
}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)
	name := nameFromRD(d)
	config := configFromRD(d)
	if !kc.TryUntil(
		func() bool {
			_, err := c.GetAll()
			return err == nil
		},
		5*time.Minute,
	) {
		return fmt.Errorf("timed out trying to connect to kafka-connect server at %s", c.URL)
	}
	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	connectorResponse, err := c.CreateConnector(req, true)
	fmt.Printf("[INFO] Created the connector %v\n", connectorResponse)

	if err == nil {
		d.SetId(name)
	}

	return err
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

	fmt.Printf("[INFO] Deleing the connector %s\n", name)

	_, err := c.DeleteConnector(req, true)
	if err == nil {
		d.SetId("")
	}

	return err
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

	if err == nil {
		log.Printf("[INFO] Config updated %v", conn.Config)
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
		log.Printf("[INFO] found the config %v", conn.Config)
		d.Set("config", conn.Config)
	}

	return err
}

func configFromRD(d *schema.ResourceData) map[string]string {
	cfg := d.Get("config").(map[string]interface{})
	config := make(map[string]string)

	for k, v := range cfg {
		switch v := v.(type) {
		case string:
			config[k] = v
		}
	}

	return config
}
