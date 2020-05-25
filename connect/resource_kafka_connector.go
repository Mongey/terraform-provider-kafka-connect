package connect

import (
	"errors"
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
			State: setNameFromID,
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
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Sensitive:   true,
				Description: "A map of string k/v attributes which are sensitive, such as passwords.",
			},
		},
	}
}

func setNameFromID(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {

	connectorName := d.Id()
	log.Printf("Import connector with name: %s", connectorName)
	d.Set("name", connectorName)

	return []*schema.ResourceData{d}, nil
}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)
	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)
	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is the mandatory field indentical to the resource name")
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

	if err != nil {
		return err
	}

	return connectorRead(d, meta)
}

func connectorDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)
	req := kc.ConnectorRequest{
		Name: name,
	}

	fmt.Printf("[INFO] Deleting the connector %s\n", name)

	_, err := c.DeleteConnector(req, true)
	if err == nil {
		d.SetId("")
	}

	return nil
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)
	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is the mandatory field indentical to the resource name")
	}

	log.Printf("[INFO] Requesting update to connector %v", name)
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
		//log.Printf("[INFO] Full config received from update is: %v", conn.Config)
		log.Printf("[INFO] Local config nonsensitive updated to: %v", newConfFiltered)
		//log.Printf("[INFO] Local config_sensitive updated to:  %v", sensitiveCache)
		d.Set("config", newConfFiltered)
		d.Set("config_sensitive", sensitiveCache)
	}

	if err != nil {
		return err
	}

	return connectorRead(d, meta)
}

func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	config, sensitiveCache := configFromRD(d)
	name := d.Get("name").(string)
	req := kc.ConnectorRequest{
		Name: name,
	}

	log.Printf("[INFO] Attempting to read remote data for connector %s", name)
	log.Printf("[INFO] Current local config nonsensitive values are: %v", config)
	//log.Printf("[INFO] Current local config_sensitive values are: %v", sensitiveCache)
	conn, err := c.GetConnector(req)

	if err == nil {
		// we do not want the sensitive values to appear in the non-masked 'config' field
		// use cached sensitive values to get the correct keys to remove from the newly read config
		newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)
		d.Set("config_sensitive", sensitiveCache)
		d.Set("config", newConfFiltered)
		log.Printf("[INFO] Local config nonsensitive data updated to %v", newConfFiltered)
		//log.Printf("[INFO] Local config_sensitive data updated to %v", sensitiveCache)
	}

	return nil
}

// Returns a full config (inclusive of sensitive values) and a config of just the sensitive values
// The first is intended to be passed to CreateConnectorRequest
// The second is intended to preserve knowledge of which keys are sensitive information in the incoming
// ConnectorResponse.Config
func configFromRD(d *schema.ResourceData) (map[string]interface{}, map[string]interface{}) {
	cfg := mapFromRD(d, "config")
	scfg := mapFromRD(d, "config_sensitive")
	config := combineMaps(cfg, scfg)
	return config, scfg
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
}

func mapFromRD(d *schema.ResourceData, key string) map[string]interface{} {
	return d.Get(key).(map[string]interface{})
}

// if there are duplicate keys this will always take the kv from second!!!
func combineMaps(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	union := make(map[string]interface{})
	for k, v := range first {
		union[k] = v
	}
	for k, v := range second {
		union[k] = v
	}
	return union
}

func removeSecondKeysFromFirst(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	for k := range second {
		delete(first, k)
	}
	return first
}
