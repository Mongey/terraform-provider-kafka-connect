package main

import (
	c "github.com/Mongey/terraform-provider-kafka-connect/connect"
	"github.com/hashicorp/terraform-plugin-sdk/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{ProviderFunc: c.Provider})
}
