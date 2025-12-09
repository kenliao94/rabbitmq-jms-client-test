package com.sample.rabbitmq.jmsClient;

import com.rabbitmq.jms.admin.RMQDestination;
import jakarta.jms.*;
import com.rabbitmq.jms.admin.RMQConnectionFactory;

public class PublishToAMQPStreamTest {

    public static void run() throws Exception {
        Connection connection = null;
        try {
            RMQConnectionFactory factory = ConfigUtil.createConnectionFactory();
            connection = factory.createConnection();
            connection.start();

            String queueName = "test-stream-jms";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            java.util.Map<String, Object> queueArgs = new java.util.HashMap<>();
            queueArgs.put("x-queue-type", "stream");
            RMQDestination destination = new RMQDestination(queueName, true, false, queueArgs);

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            String msg_content = "Hello World!!";
            TextMessage textMessage = session.createTextMessage(msg_content);
            producer.send(textMessage);

            System.out.printf("Published to AMQP queue '%s': %s", queueName, msg_content);

            producer.close();
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
