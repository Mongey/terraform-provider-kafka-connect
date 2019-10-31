module github.com/Mongey/terraform-provider-kafka-connect

go 1.12

require (
	github.com/hashicorp/terraform v0.12.1
	github.com/ricardo-ch/go-kafka-connect v0.0.0-20180209135343-132b7b7ad380
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
