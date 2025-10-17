package com.sample.rabbitmq.jmsClient;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates point-to-point messaging using a queue
 */
public class QueueTestcase {
    
    public static void run() throws Exception {
        Connection connection = null;
        
        try {
            // Create a connection factory
            RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
            connectionFactory.setHost(Constants.RABBITMQ_HOST);
            connectionFactory.setPort(Constants.RABBITMQ_PORT);
            connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
            connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
            connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
            connectionFactory.useSslProtocol();
            
            // Create a connection
            connection = connectionFactory.createConnection();
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException e) {
                    System.err.println("JMS Connection Exception: " + e.getMessage());
                }
            });
            connection.start();
            
            // Create a non-transacted, auto-acknowledged session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create a queue destination
            Queue queue = session.createQueue(Constants.QUEUE_NAME);
            
            // Create a message producer
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            // Send 5 text messages
            for (int i = 1; i <= 5; i++) {
                String messageText = "Queue message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                
                // Set some properties
                message.setIntProperty("MessageNumber", i);
                message.setStringProperty("Source", "RabbitMQ JMS Sample");
                
                // Send the message
                producer.send(message);
                System.out.println("Sent message: " + messageText);
            }
            
            // Create a message consumer
            MessageConsumer consumer = session.createConsumer(queue);
            
            // Receive messages synchronously
            System.out.println("\\nReceiving messages synchronously:");
            for (int i = 1; i <= 5; i++) {
                Message receivedMessage = consumer.receive(5000); // 5 second timeout
                
                if (receivedMessage instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) receivedMessage;
                    System.out.println("Received: " + textMessage.getText());
                    System.out.println("  MessageNumber property: " + textMessage.getIntProperty("MessageNumber"));
                    System.out.println("  Source property: " + textMessage.getStringProperty("Source"));
                } else if (receivedMessage == null) {
                    System.out.println("No message received within timeout period");
                    break;
                } else {
                    System.out.println("Received message of type: " + receivedMessage.getClass().getName());
                }
            }
            
            // Clean up resources
            consumer.close();
            producer.close();
            session.close();
            
        } finally {
            // Always close the connection
            if (connection != null) {
                connection.close();
            }
        }
    }
}