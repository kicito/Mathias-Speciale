type UpdateNumberRequest {
    .username : string
}

type LocalUpdateResponse {
    .code: int
    .reason: string
}

type UpdateNumberResponse: string

interface ServiceAInterface{
    RequestResponse:
        updateNumber( UpdateNumberRequest )( UpdateNumberResponse )
    OneWay:
        finalizeChoreography( long )
}