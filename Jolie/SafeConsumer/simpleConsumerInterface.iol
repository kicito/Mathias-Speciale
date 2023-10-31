type UpdateDatabaseRequest{
    .userToUpdate: string
    .sid[0, 1]: string
}

interface SimpleConsumerInterface{
    RequestResponse:
        updateNumberForUser( UpdateDatabaseRequest )( string )
}