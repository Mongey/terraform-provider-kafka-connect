provider "kafka-connect" {
  url = "http://localhost:8083"
}

resource "kafka-connect_connector" "sqlite-sink" {
  name = "sqlite-sink"

  configuration = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = 1
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }

  config_sensitive = {
    "password" = "whooo-this-should-never-appear-unmasked"
    "mash" = "nice"
  }
}
