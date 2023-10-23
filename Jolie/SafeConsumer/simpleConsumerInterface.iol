type UpdateDatabaseRequest{
    .userToUpdate: string
}

type UpdateDatabaseResponse {
    .code: int
    .reason: string
}

interface SimpleConumerInterface{
    RequestResponse:
        UpdateNumberForUser( UpdateDatabaseRequest )( UpdateDatabaseResponse )
}