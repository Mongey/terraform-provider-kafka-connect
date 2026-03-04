package connect

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
)

const (
	ConnectorStatusRunning    = "RUNNING"
	ConnectorStatusPaused     = "PAUSED"
	ConnectorStatusUnassigned = "UNASSIGNED"
	ConnectorStatusFailed     = "FAILED"
)

func resourceKafkaConnectorStatus() *schema.Resource {
	return &schema.Resource{
		CreateContext: CreateKafkaConnectorStatus,
		ReadContext:   ReadKafkaConnectorStatus,
		UpdateContext: UpdateKafkaConnectorStatus,
		DeleteContext: DeleteKafkaConnectorStatus,
		Importer: &schema.ResourceImporter{
			StateContext: ImportKafkaConnectorStatus,
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Second),
			Update: schema.DefaultTimeout(60 * time.Second),
			Delete: schema.DefaultTimeout(60 * time.Second),
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the connector",
			},
			"state": {
				Type:     schema.TypeString,
				Required: true,
				ValidateFunc: validation.StringInSlice([]string{
					ConnectorStatusRunning,
					ConnectorStatusPaused,
				}, false),
			},
		},
	}
}

func CreateKafkaConnectorStatus(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(kc.HighLevelClient)
	connectorName := nameFromRD(d)
	state := stateFromRD(d)

	req := kc.ConnectorRequest{
		Name: connectorName,
	}

	var err error
	if state == ConnectorStatusRunning {
		_, err = c.ResumeConnector(req, true)
	} else if state == ConnectorStatusPaused {
		_, err = c.PauseConnector(req, true)
	} else {
		return diag.Errorf("unknown desired state: %s. Can only set to %s or %s", state, ConnectorStatusRunning, ConnectorStatusPaused)
	}

	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[INFO] Connector status changed to %s\n", state)

	d.SetId(connectorName)
	if err = d.Set("state", state); err != nil {
		return diag.FromErr(err)
	}

	return ReadKafkaConnectorStatus(ctx, d, meta)
}

func ReadKafkaConnectorStatus(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)
	req := kc.ConnectorRequest{
		Name: name,
	}

	var connectorStatusResponse kc.GetConnectorStatusResponse
	var err error
	log.Printf("[INFO] Attempting to read status for connector %s", name)
	if connectorStatusResponse, err = c.GetConnectorStatus(req); err != nil {
		return diag.FromErr(err)
	}

	if status := connectorStatusResponse.Code; status == 404 {
		log.Printf("[WARN] Connector %s not found, removing from state", name)
		d.SetId("")
		return nil
	}

	if connectorState := connectorStatusResponse.ConnectorStatus["state"]; connectorState != "" {
		switch connectorState {
		case ConnectorStatusRunning:
			err = d.Set("state", ConnectorStatusRunning)
			log.Printf("[INFO] Connector state updated to %s", ConnectorStatusRunning)
		case ConnectorStatusPaused:
			err = d.Set("state", ConnectorStatusPaused)
			log.Printf("[INFO] Connector state updated to %s", ConnectorStatusPaused)
		case ConnectorStatusFailed:
			err = d.Set("state", ConnectorStatusFailed)
			log.Printf("[INFO] Connector is failing and will be registered as failed")
		case ConnectorStatusUnassigned:
			err = d.Set("state", ConnectorStatusRunning)
			log.Printf("[INFO] Connector is unassigned and will be registered as running instead")
		default:
			log.Printf("[ERROR] Unknown connector status %s", connectorState)
		}

		if err != nil {
			return diag.FromErr(err)
		}
	}

	return nil
}

func UpdateKafkaConnectorStatus(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(kc.HighLevelClient)

	connectorName := nameFromRD(d)

	log.Printf("[INFO] Requesting update to connector status %s", connectorName)
	req := kc.ConnectorRequest{
		Name: connectorName,
	}

	var err error
	if d.HasChange("state") {
		pState, dState := d.GetChange("state")
		previousState := pState.(string)
		desiredState := dState.(string)

		if desiredState == ConnectorStatusRunning && previousState == ConnectorStatusFailed {
			log.Printf("[INFO] Attempting to restart connector %s", connectorName)
			err = withRebalanceRetry(func() error {
				_, err = c.RestartConnector(req)
				return err
			}, d.Timeout(schema.TimeoutUpdate))
		} else if desiredState == ConnectorStatusRunning && previousState == ConnectorStatusPaused {
			log.Printf("[INFO] Attempting to resume connector %s", connectorName)
			_, err = c.ResumeConnector(req, true)
		} else if desiredState == ConnectorStatusPaused {
			log.Printf("[INFO] Pausing connector %s", connectorName)
			_, err = c.PauseConnector(req, true)
		}

		if err != nil {
			log.Printf("[ERROR] Failed to update connector status %s: %s", connectorName, err)
			return diag.FromErr(err)
		}

		log.Printf("[INFO] Connector status changed to %s\n", desiredState)
	}

	return ReadKafkaConnectorStatus(ctx, d, meta)
}

func DeleteKafkaConnectorStatus(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	log.Printf("[INFO] Deleting status resource for connector '%s'; the connector itself will not be paused or resumed", d.Id())
	d.SetId("")
	return nil
}

func ImportKafkaConnectorStatus(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {

	connectorName := d.Id()
	log.Printf("[INFO] Importing connector status for: %s", connectorName)
	if err := d.Set("name", connectorName); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}

func stateFromRD(d *schema.ResourceData) string {
	return d.Get("state").(string)
}
