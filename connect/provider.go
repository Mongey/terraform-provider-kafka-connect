package connect

import (
	"context"
	"crypto/tls"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"gopkg.in/resty.v1"

	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
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
			"tls_root_ca_file": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_ROOT_CA_FILE", ""),
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
			"tls_auth_is_insecure": {
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_IS_INSECURE", ""),
			},
			"headers": {
				Type: schema.TypeMap,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Optional: true,
				// No DefaultFunc here to read from the env on account of this issue:
				// https://github.com/hashicorp/terraform-plugin-sdk/issues/142
			},
		},
		ConfigureContextFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"kafka-connect_connector": kafkaConnectorResource(),
		},
	}
	log.Printf("[INFO] Created provider: %v", provider)
	return &provider
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	log.Printf("[INFO] Initializing KafkaConnect client")
	addr := d.Get("url").(string)
	c := kc.NewClient(addr)
	user := d.Get("basic_auth_username").(string)
	pass := d.Get("basic_auth_password").(string)
	if user != "" && pass != "" {
		c.SetBasicAuth(user, pass)
	}

	tls_root_ca_file := d.Get("tls_root_ca_file").(string)
	if tls_root_ca_file != "" {
		resty.SetRootCertificate(tls_root_ca_file)
	}

	crt := d.Get("tls_auth_crt").(string)
	key := d.Get("tls_auth_key").(string)
	is_insecure := d.Get("tls_auth_is_insecure").(bool)
	log.Printf("[INFO]Cert : %s\nKey: %s", crt, key)
	log.Printf("[INFO]SSl connection is insecure : %t", is_insecure)

	if crt != "" && key != "" {
		cert, err := tls.LoadX509KeyPair(crt, key)
		if err != nil {
			log.Fatalf("client: loadkeys: %s", err)
		} else {
			if is_insecure {
				c.SetInsecureSSL()
			}
			c.SetClientCertificates(cert)
		}
	}

	headers := d.Get("headers").(map[string]interface{})
	if headers != nil {
		for k, v := range headers {
			c.SetHeader(k, v.(string))
		}
	}

	return c, nil
}
