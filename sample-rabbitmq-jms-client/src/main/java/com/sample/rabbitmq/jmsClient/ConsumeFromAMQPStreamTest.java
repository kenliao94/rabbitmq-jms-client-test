package com.sample.rabbitmq.jmsClient;

import jakarta.jms.*;
import com.rabbitmq.jms.admin.RMQConnectionFactory;
import com.rabbitmq.jms.admin.RMQDestination;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumeFromAMQPStreamTest {

    public static void run() throws Exception {
        Connection connection = null;
        try {
            RMQConnectionFactory factory = ConfigUtil.createConnectionFactory();
            connection = factory.createConnection();

            String queueName = "test-stream-jms";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            RMQDestination destination = new RMQDestination();
            destination.setDestinationName(queueName);
            destination.setAmqp(true);
            destination.setAmqpQueueName(queueName);
            
            MessageConsumer consumer = session.createConsumer(destination);

            int maxMessages = 10;
            AtomicInteger count = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(maxMessages);

            consumer.setMessageListener(message -> {
                try {
                    int msgNum = count.incrementAndGet();
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        System.out.printf("Message %d: %s%n", msgNum, textMessage.getText());
                    } else if (message instanceof BytesMessage) {
                        BytesMessage bytesMessage = (BytesMessage) message;
                        byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                        bytesMessage.readBytes(bytes);
                        String content = new String(bytes);
                        System.out.printf("Message %d: %s%n", msgNum, content);
                    } else {
                        System.out.printf("Message %d: [%s]%n", msgNum, message.getClass().getSimpleName());
                    }
                    latch.countDown();
                } catch (JMSException e) {
                    System.err.printf("Error processing message: %s%n", e.getMessage());
                }
            });

            connection.start();
            System.out.printf("Consuming from AMQP stream '%s' using MessageListener...%n", queueName);

            latch.await(3600, TimeUnit.SECONDS);

            System.out.printf("Total messages consumed from AMQP stream: %d%n", count.get());

            consumer.close();
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
