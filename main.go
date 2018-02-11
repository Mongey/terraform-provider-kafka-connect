package main

import (
	kc "github.com/Mongey/terraform-provider-kafka-connect/kc"
	"github.com/hashicorp/terraform/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{ProviderFunc: kc.Provider})
}
