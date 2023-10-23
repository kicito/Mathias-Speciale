type InboxUpdatedResponse {
    .amountMessagesRead: int
}

interface ServiceBInterface {
    RequestResponse:
        numbersUpdated( string )( InboxUpdatedResponse )
}
