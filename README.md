# `terraform-plugin-kafka-connect`
[![CircleCI](https://circleci.com/gh/Mongey/terraform-provider-kafka-connect.svg?style=svg)](https://circleci.com/gh/Mongey/terraform-provider-kafka-connect)

A [Terraform][1] plugin for managing [Apache Kafka Connect][2].

## Installation

Download and extract the [latest
release](https://github.com/Mongey/terraform-provider-kafka-connect/releases/latest) to
your [terraform plugin directory][third-party-plugins] (typically `~/.terraform.d/plugins/`)

## Example

Configure the provider directly, or set the ENV variable `KAFKA_CONNECT_URL`
```hcl
provider "kafka-connect" {
  url = "http://localhost:8083"
  basic_auth_username = "user" # Optional
  basic_auth_password = "password" # Optional
}

resource "kafka-connect_connector" "sqlite-sink" {
  name = "sqlite-sink"

  config = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = 1
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
    "connection.user" = "admin"
  }

  config_sensitive = {
    "connection.password" = "this-should-never-appear-unmasked"
  }
}
```

## Provider Properties

| Property              | Type   | Example                 | Alternative environment variable name |
|-----------------------|--------|-------------------------|---------------------------------------|
| `url`                 | URL    | "http://localhost:8083" | `KAFKA_CONNECT_URL`                   |
| `basic_auth_username` | String | "user"                  | `KAFKA_CONNECT_BASIC_AUTH_USERNAME`   |
| `basic_auth_password` | String | "password"              | `KAFKA_CONNECT_BASIC_AUTH_PASSWORD`   |

## Resource Properties

| Property              | Type      | Description                                                          |
|-----------------------|-----------|----------------------------------------------------------------------|
| `name`                | String    | Connector name                                                       |
| `config`              | HCL Block | Connector configuration                                              |
| `config_sensitive`    | HCL Block | Sensitive connector configuration. Will be masked in output.         |

## Developing

0. [Install go][install-go]
0. Clone repository to: `$GOPATH/src/github.com/Mongey/terraform-provider-kafka-connect`
    ``` bash
    mkdir -p $GOPATH/src/github.com/Mongey/terraform-provider-kafka-connect; cd $GOPATH/src/github.com/Mongey/
    git clone https://github.com/Mongey/terraform-provider-kafka-connect.git
    ```
0. Build the provider `make build`
0. Run the tests `make test`
0. Run acceptance tests `make testacc`

[1]: https://www.terraform.io
[2]: https://kafka.apache.org/documentation/#connect
[third-party-plugins]: https://www.terraform.io/docs/configuration/providers.html#third-party-plugins
[install-go]: https://golang.org/doc/install#install
