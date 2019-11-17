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
			"configuration": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
			},
			"config_sensitive": {
				Type: schema.TypeMap,
				Optional: true,
				ForceNew: false,
				Sensitive: false,
				Description: "A map of string k/v attributes which are sensitive, such as passwords.",
			},
		},
	}
}

var sensitiveCache = map[string]string{}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)
	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)
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
		newConfFiltered := removeSecondKeysFromFirst(connectorResponse.Config, sensitiveCache)
		d.SetId(name)
		d.Set("config_sensitive", sensitiveCache)
		d.Set("config", newConfFiltered)
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

	fmt.Printf("[INFO] Deleting the connector %s\n", name)

	_, err := c.DeleteConnector(req, true)
	if err == nil {
		d.SetId("")
	}

	return err
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)


	log.Printf("Passing full config into request to Kafka Connect: %v", config)
	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	log.Printf("[INFO] Looking for %s", name)
	conn, err := c.UpdateConnector(req, true)

	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)

	if err == nil {
		log.Printf("[INFO] Full config received from update was %v", conn.Config)
		log.Printf("[INFO] Configuration updated %v", newConfFiltered)
		log.Printf("[INFO] Config sensitive updated %v", sensitiveCache)
		d.Set("configuration", newConfFiltered)
		d.Set("config_sensitive", sensitiveCache)
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
	log.Printf("[INFO] My old configuration values are: %v", d.Get("configuration"))
	log.Printf("[INFO] My old config_sensitive values are: %v", d.Get("config_sensitive"))
	conn, err := c.GetConnector(req)

	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)

	if err == nil {
		log.Printf("[INFO] found the config %v", conn.Config)
		d.Set("config_sensitive", sensitiveCache)
		d.Set("configuration", newConfFiltered)
		log.Printf("[INFO] Set the new configuration to %v", newConfFiltered)
		log.Printf("[INFO] Set the new config_sensitive to %v", sensitiveCache)
	}

	return err
}


func configFromRD(d *schema.ResourceData) (map[string]string, map[string]string) {
	cfg := mapFromRD(d, "configuration")
	scfg := mapFromRD(d, "config_sensitive")
	sensitiveCache = scfg
	config := combineMaps(cfg, scfg)
	return config, scfg
}

func mapFromRD(d *schema.ResourceData, key string) map[string]string {
	mapToBe := d.Get(key).(map[string]interface{})
	realMap := make(map[string]string)
	for k, v := range mapToBe {
		realMap[k] = v.(string)
	}
	return realMap
}

// if there are duplicate keys this will always take the kv from second!!!
func combineMaps(first map[string]string, second map[string]string) map[string]string {
	union := make(map[string]string)
	for k, v := range first {
		union[k] = v
	}
	for k, v := range second {
		union[k] = v
	}
	return union
}

func removeSecondKeysFromFirst(first map[string]string, second map[string]string) map[string]string {
	for k := range second {
		delete(first, k)
	}
	return first
}
