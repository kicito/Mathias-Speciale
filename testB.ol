include "database.iol"
include "console.iol"

from .testA import ServiceAInterface
from runtime import Runtime

type UR: void {
    .sid: string
}

interface ServiceBInterface {
    RequestResponse: update( UR )( string )
}

type ServiceBProperties{
    .serviceALocation: any
}

service ServiceB(p: ServiceBProperties){
    execution: concurrent
    inputPort ServiceBLocal {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceBInterface
    }

    outputPort ServiceA {
        location: "local" 
        protocol: http{
            format = "json"
        }
        interfaces: ServiceAInterface
    }
    embed Runtime as Runtime

    init {
        ServiceA.location = p.serviceALocation

        with ( connectionInfo )
        {
            .username = "";
            .password = "";
            .host = "";
            .database = "file:test.sqlite"; // "." for memory-only
            .driver = "sqlite"
        }

        connect@Database(connectionInfo)(res)
    }

    main {
        [update( req ) ( res ){
            println@Console( "Hello from Service B" )(  )
            interruptRequest.sid << req.sid 
            interrupt@ServiceA( interruptRequest )( res )
            res = "Nice"
        }]
    }
}