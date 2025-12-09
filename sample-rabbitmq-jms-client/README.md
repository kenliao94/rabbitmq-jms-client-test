# RabbitMQ JMS Client Sample

Sample application demonstrating RabbitMQ JMS client usage with AMQP queues and streams.

## Prerequisites

- Java 11+
- Maven 3.6+
- RabbitMQ server running locally
- Local RabbitMQ JMS client 3.5.0-SNAPSHOT

## Setup

### 1. Install Local RabbitMQ JMS Client

Run the installation script to copy and install the local RabbitMQ JMS client:

```bash
./install-local-rabbitmq-jms.sh
```

This script:
- Copies `rabbitmq-jms-3.5.0-SNAPSHOT.jar` from the source project
- Installs it to your local Maven repository with dependencies

### 2. Configure RabbitMQ Connection

Edit `.env` file with your RabbitMQ connection details:

```properties
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_VIRTUAL_HOST=/
```

## Build

```bash
mvn clean package
```

## Run

```bash
mvn exec:java
```

Or run the shaded JAR:

```bash
java -jar target/sample-rabbitmq-jms-client-1.0-SNAPSHOT.jar
```

## Test Cases

### PublishToAMQPQueueTest
Publishes messages to a RabbitMQ stream queue using JMS.

### ConsumeFromAMQPQueueTest
Consumes messages synchronously from an AMQP queue using `receive()`.

### ConsumeFromAMQPStreamTest
Consumes messages asynchronously from an AMQP stream using `MessageListener`.


## Switching Test Cases

Edit `App.java` to run different test cases:

```java
public static void main(String[] args) {
    try {
        // Uncomment the test you want to run:
        // QueueTestcase.run();
        // PublishToAMQPQueueTest.run();
        // ConsumeFromAMQPQueueTest.run();
        ConsumeFromAMQPStreamTest.run();
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```

## Project Structure

```
src/main/java/com/sample/rabbitmq/jmsClient/
├── App.java                          # Main entry point
├── ConfigUtil.java                   # Connection factory configuration
├── Constants.java                    # Application constants
├── PublishToAMQPQueueTest.java      # Stream publishing example
├── ConsumeFromAMQPQueueTest.java    # Synchronous consumption example
├── ConsumeFromAMQPStreamTest.java   # Asynchronous consumption example
└── QueueTestcase.java               # Basic queue example
```
