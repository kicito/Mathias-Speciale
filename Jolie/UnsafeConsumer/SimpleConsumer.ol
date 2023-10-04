
include "database.iol"
include "console.iol"
include "time.iol"

from .inboxService import Inbox

interface SimpleConumerInterface{
}

service SimpleConsumer{
    inputPort SimpleProducer {
        location: "socket://localhost:8082" 
        protocol: http{
            format = "json"
        }
        interfaces: SimpleConumerInterface
    }
    embed Inbox as Inbox

    main {
        Initialize@Inbox("Hi")
        linkIn( Inbox )
    }
}

