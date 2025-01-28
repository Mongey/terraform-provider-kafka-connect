module github.com/Mongey/terraform-provider-kafka-connect

go 1.16

require (
	bou.ke/monkey v1.0.2 // indirect
	github.com/hashicorp/terraform-plugin-sdk/v2 v2.10.1
	github.com/ricardo-ch/go-kafka-connect/v3 v3.0.0-20221117134721-e033f95963cb
	gopkg.in/resty.v1 v1.12.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
