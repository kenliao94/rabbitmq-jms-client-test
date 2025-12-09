package com.sample.rabbitmq.jmsClient;

import jakarta.jms.*;
import com.rabbitmq.jms.admin.RMQConnectionFactory;
import com.rabbitmq.jms.admin.RMQDestination;

public class ConsumeFromAMQPQueueTest {

    public static void run() throws Exception {
        Connection connection = null;
        try {
            RMQConnectionFactory factory = ConfigUtil.createConnectionFactory();
            connection = factory.createConnection();
            connection.start();

            String queueName = "test-stream-jms";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            RMQDestination destination = new RMQDestination();
            destination.setDestinationName(queueName);
            destination.setAmqp(true);
            destination.setAmqpQueueName(queueName);
            
            MessageConsumer consumer = session.createConsumer(destination);

            System.out.printf("Consuming from AMQP queue '%s'...%n", queueName);
            int count = 0;
            int maxMessages = 10;
            long timeoutMs = 3600 * 1000;

            while (count < maxMessages) {
                Message message = consumer.receive(timeoutMs);
                if (message == null) {
                    System.out.println("No more messages (timeout)");
                    break;
                }

                count++;
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.printf("Message %d: %s%n", count, textMessage.getText());
                } else if (message instanceof BytesMessage) {
                    BytesMessage bytesMessage = (BytesMessage) message;
                    byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(bytes);
                    String content = new String(bytes);
                    System.out.printf("Message %d: %s%n", count, content);
                } else {
                    System.out.printf("Message %d: [%s]%n", count, message.getClass().getSimpleName());
                }
            }

            System.out.printf("Total messages consumed from AMQP queue: %d%n", count);

            consumer.close();
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
