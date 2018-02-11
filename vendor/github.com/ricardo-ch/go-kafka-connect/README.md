# go-kafka-connect
Go project containing two different sub-projects: a kafka-connect client library and a CLI to use it.

## Kafka-connect client library
This library is to be used as an abstraction layer for the kafka-connect REST API.
It currently implements the following API calls:
- Create a connector
- Update a connector
- Delete a connector
- Pause a connector
- Resume a connector
- Restart a connector
- Get a connector's details (overview, configuration, status or tasks list)

It also contains two 'bonus' features:
- Do synchronously: All calls to the REST API trigger an asynchronous function on kafka-connect.
  This feature lets the library check regularly if the action has taken effect on kafka-connect's side,
  and considers the request as completed only when the consequences of the command can be verified.
  It allows users of this library to use its functions in a synchronous way.
- Deploy connector, a function used to deploy a connector, or replace an existing one gracefully.
  This function checks if the target connector exists. If it exists, it will then be paused, and deleted.
  The new connector is then deployed, and resumed. This function is always synchronous.


# Setup environment
Required:
 - Go 1.9
 - Docker (for testing purpose only)

run `make install` to install dependencies


# Testing
For now, only integration test are available.
run `docker-compose up` then wait patiently until it boots and run `make test-integration`

Right now, integration tests only run locally