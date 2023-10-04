interface  SimpleKafkaConnectorInterface {
    OneWay: updateCountForUsername ( string )
}

service SimpleKafkaConnector{
    inputPort Input {
        Location: "local"
        Interfaces: SimpleKafkaConnectorInterface
        } 
        foreign java {
            class: "example.KafkaRelayer"
        }
}