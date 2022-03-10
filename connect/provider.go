package connect

import (
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	kc "github.com/ricardo-ch/go-kafka-connect/lib/connectors"
)

func Provider() *schema.Provider {
	log.Printf("[INFO] Creating Provider")
	provider := schema.Provider{
		Schema: map[string]*schema.Schema{
			"url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_URL", ""),
			},
			"basic_auth_username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_USERNAME", ""),
			},
			"basic_auth_password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_PASSWORD", ""),
			},
			"tls_auth_crt": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_CRT", ""),
			},
			"tls_auth_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_KEY", ""),
			},
		},
		ConfigureFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"kafka-connect_connector": kafkaConnectorResource(),
		},
	}
	log.Printf("[INFO] Created provider: %v", provider)
	return &provider
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	log.Printf("[INFO] Initializing KafkaConnect client")
	addr := d.Get("url").(string)
	c := kc.NewClient(addr)
	user := d.Get("basic_auth_username").(string)
	pass := d.Get("basic_auth_password").(string)
	if user != "" && pass != "" {
		c.SetBasicAuth(user, pass)
	}
	
	crt := d.Get("tls_auth_crt").(string)
	key := d.Get("tls_auth_key").(string)
	log.Printf("[INFO]Cert : %s\nKey: %s", crt, key)

	if crt != "" && key != "" {
		cert, err := tls.LoadX509KeyPair(crt, key)
		if err != nil {
			log.Fatalf("client: loadkeys: %s", err)
		} else {
			c.SetInsecureSSL()
			c.SetClientCertificates(cert)
		}
	}
	return c, nil
}
