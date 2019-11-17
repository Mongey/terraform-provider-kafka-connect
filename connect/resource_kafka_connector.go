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

	if err == nil {
		newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)
		log.Printf("[INFO] Full config received from update is: %v", conn.Config)
		log.Printf("[INFO] Local config nonsensitive updated to: %v", newConfFiltered)
		//log.Printf("[INFO] Local config_sensitive updated to:  %v", sensitiveCache)
		d.Set("config", newConfFiltered)
		d.Set("config_sensitive", sensitiveCache)
	}

	return err
}

func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.Client)

	config, sensitiveCache := configFromRD(d)
	name := d.Get("name").(string)
	req := kc.ConnectorRequest{
		Name: name,
	}

	log.Printf("[INFO] Attempting to read remote data for connector %s", name)
	log.Printf("[INFO] Current local config nonsensitive values are: %v", config)
	//log.Printf("[INFO] Current local config_sensitive values are: %v", sensitiveCache)
	conn, err := c.GetConnector(req)

	// we do not want the sensitive values to appear in the non-masked 'config' field
	// use cached sensitive values to get the correct keys to remove from the newly read config
	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)

	if err == nil {
		d.Set("config_sensitive", sensitiveCache)
		d.Set("config", newConfFiltered)
		log.Printf("[INFO] Local config nonsensitive data updated to %v", newConfFiltered)
		//log.Printf("[INFO] Local config_sensitive data updated to %v", sensitiveCache)
	}

	return err
}

// Returns a full config (inclusive of sensitive values) and a config of just the sensitive values
// The first is intended to be passed to CreateConnectorRequest
// The second is intended to preserve knowledge of which keys are sensitive information in the incoming
// ConnectorResponse.Config
func configFromRD(d *schema.ResourceData) (map[string]string, map[string]string) {
	cfg := mapFromRD(d, "config")
	scfg := mapFromRD(d, "config_sensitive")
	config := combineMaps(cfg, scfg)
	return config, scfg
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
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
