module github.com/Mongey/terraform-provider-kafka-connect

go 1.12

require (
	bou.ke/monkey v1.0.2 // indirect
	github.com/hashicorp/terraform v0.12.26 // indirect
	github.com/hashicorp/terraform-plugin-sdk v1.0.0
	github.com/ricardo-ch/go-kafka-connect v0.0.0-20200403115642-f7b66cb04ed7
	gopkg.in/resty.v1 v1.12.0 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
