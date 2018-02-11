A [Terraform][1] plugin for managing [Apache Kafka Connect][2].

[![CircleCI](https://circleci.com/gh/Mongey/terraform-provider-kafka-connect.svg?style=svg)](https://circleci.com/gh/Mongey/terraform-provider-kafka-connect)

# Example

```hcl
provider "kc" {
  url = "http://localhost:8080"
}

resource "kc_connector" "sqlite-sink" {
  name = "sqlite-sink"

  config = {
    "name"            = "test-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
```

[1]: https://www.terraform.io
[2]: https://kafka.apache.org/documentation/#connect
