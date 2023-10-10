This directory contains an example of a simple choreography, in which two services communicates through a Kafka server.
The Outbox pattern has been implemented, providing at-least-once delivery into Kafka. Seeing as kafka works as persistent storage, keeping track of the last offset each user has read, implementing the inbox pattern was not nessecary to make this example also have at-least-once delivery from Kafka into the services. This is still to-be-done, as implementing the Inbox Pattern is more difficult than I initially assumed.

1. serviceA.updateLocally -> Kafka.localUpdateAtServiceA;
2. Kafka.localUpdateAtServiceA -> serviceB.matchAUpdate;
3. serviceB.matchAUpdate -> Kafka.localUpdateAtServiceB;
4. Kafka.localUpdateAtServiceB -> serviceA.finishChoreography;

To run the example, all that is needed is Docker compose:
1. Download the docker-compose file containing Kafka and the two services (`wget -O docker-compose.yaml https://raw.githubusercontent.com/Maje419/Speciale/main/Example/docker-compose.yml?token=GHSAT0AAAAAACHPMOWQF6PK2LYOUHIS72H4ZJFHDNA`)
2. Run `docker-compose up`. serviceA now listens on `localhost:8080`.
3. From a seperate terminal, run `curl http://localhost:8080/updateNumber?username=user1` to initiate the choreography of updating the number connected to user 'user1'
