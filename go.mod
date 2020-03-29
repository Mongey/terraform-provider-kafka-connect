module github.com/Mongey/terraform-provider-kafka-connect

go 1.12

require (
	github.com/hashicorp/terraform v0.12.1
	github.com/ricardo-ch/go-kafka-connect v0.0.0-20190603085745-7ed69492c725
	gopkg.in/resty.v1 v1.12.0 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
replace github.com/ricardo-ch/go-kafka-connect v0.0.0-20190603085745-7ed69492c725 => github.com/mmajis/go-kafka-connect v0.0.0-20200328184024-6284b2164d53
