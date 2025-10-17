package com.sample.rabbitmq.jmsClient;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates publish/subscribe messaging using a topic
 */
public class TopicTestcase {
    
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
            connection.start();
            
            // Create a non-transacted, auto-acknowledged session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create a topic destination
            Topic topic = session.createTopic(Constants.TOPIC_NAME);
            
            // Create a message consumer with a message listener
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            TextMessage textMessage = (TextMessage) message;
                            System.out.println("Received from topic: " + textMessage.getText());
                        } else {
                            System.out.println("Received non-text message from topic");
                        }
                    } catch (JMSException e) {
                        System.err.println("Error processing topic message: " + e.getMessage());
                    }
                }
            });
            
            // Create a message producer
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // Topics often use non-persistent messages
            
            // Send 3 messages to the topic
            for (int i = 1; i <= 3; i++) {
                String messageText = "Topic message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                producer.send(message);
                System.out.println("Sent to topic: " + messageText);
                
                // Small delay to allow the asynchronous consumer to process
                Thread.sleep(100);
            }
            
            // Wait a bit to ensure all messages are received by the asynchronous consumer
            Thread.sleep(1000);
            
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