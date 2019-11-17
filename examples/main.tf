provider "kafka-connect" {
  url = "http://localhost:8083"
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
