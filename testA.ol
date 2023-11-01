include "database.iol"
include "console.iol"

from runtime import Runtime
from .testB import ServiceB
from .testB import ServiceBInterface

type NameRequest {
    .name: string
}

type InterruptRequest {
    .sid: string
}

interface ServiceAInterface {
    RequestResponse: 
        initiateChoreography( NameRequest ) ( string ),
        interrupt( InterruptRequest ) ( string )
}

service ServiceA{
    execution: concurrent
    inputPort ServiceA {
        location: "socket://localhost:8087" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    inputPort ServiceALocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }

    outputPort ServiceB{
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }
    embed Runtime as Runtime

    init {
        getLocalLocation@Runtime(  )( localLocation )   
        loadEmbeddedService@Runtime({
            filepath = "testB.ol"
            params << {
                serviceALocation << localLocation
            }
        })( ServiceB.location )
        
        with ( connectionInfo )
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:test.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        connect@Database(connectionInfo)(res)
        update@Database( "CREATE TABLE IF NOT EXISTS testtable(name VARCHAR(50))")(  )
    }

    cset {
        token: InterruptRequest.sid
    }

    main{
        [initiateChoreography( req )( res ){
            println@Console( "Hello from Service A" )(  )
            updateRequest.sid = csets.token = new
            update@ServiceB( updateRequest )( resp )
            res = "Updated database"
        }] {
            println@Console( "Choreography finished" )(  )
        }

        [interrupt( req )( res ){
            println@Console( "Service A has been interrupted!" )(  )
            res = "Nice"
        }]
    }
}