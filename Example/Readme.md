This directory contains an example of a simple choreography, in which two services communicates through a Kafka server.
The Outbox pattern has been implemented, providing at-least-once delivery into Kafka. Seeing as kafka works as persistent storage, keeping track of the last offset each user has read, implementing the inbox pattern was not nessecary to make this example also have at-least-once delivery from Kafka into the services. This is still to-be-done, as implementing the Inbox Pattern is more difficult than I initially assumed.

serviceA.updateLocally -> Kafka.localUpdateAtServiceA;

Kafka.localUpdateAtServiceA -> serviceB.matchAUpdate;

serviceB.matchAUpdate -> Kafka.localUpdateAtServiceB;

Kafka.localUpdateAtServiceB -> serviceA.finishChoreography;

To run the example, all that is needed is Docker
