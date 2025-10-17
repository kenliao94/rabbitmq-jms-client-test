package com.sample.rabbitmq.jmsClient;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates sending non-persistent messages to a queue
 */
public class NonPersistentQueueTestcase {
    
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
            
            // Create a queue destination for non-persistent messages
            Queue queue = session.createQueue(Constants.QUEUE_NAME + ".nonpersistent");
            
            // Create a message producer with NON_PERSISTENT delivery mode
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // Explicitly set to non-persistent
            
            System.out.println("\\n=== Non-Persistent Queue Example ===");
            
            // Send 5 non-persistent text messages
            for (int i = 1; i <= 5; i++) {
                String messageText = "Non-persistent message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                
                // Set some properties
                message.setIntProperty("MessageNumber", i);
                message.setStringProperty("Persistence", "NON_PERSISTENT");
                message.setStringProperty("Source", "RabbitMQ JMS Sample");
                
                // Send the message
                producer.send(message);
                System.out.println("Sent non-persistent message: " + messageText);
            }
            
            // You can also set delivery mode per message
            TextMessage specialMessage = session.createTextMessage("Special non-persistent message");
            producer.send(specialMessage, DeliveryMode.NON_PERSISTENT, 
                          Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            System.out.println("Sent special non-persistent message with explicit delivery mode");
            
            // Create a message consumer
            MessageConsumer consumer = session.createConsumer(queue);
            
            // Receive messages synchronously
            System.out.println("\\nReceiving non-persistent messages:");
            Message receivedMessage;
            int count = 0;
            
            while ((receivedMessage = consumer.receive(2000)) != null) { // 2 second timeout
                count++;
                if (receivedMessage instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) receivedMessage;
                    System.out.println("Received: " + textMessage.getText());
                    
                    // Check if this message has properties
                    try {
                        if (textMessage.propertyExists("MessageNumber")) {
                            System.out.println("  MessageNumber property: " + textMessage.getIntProperty("MessageNumber"));
                            System.out.println("  Persistence property: " + textMessage.getStringProperty("Persistence"));
                        } else {
                            System.out.println("  (Special message without numbered properties)");
                        }
                    } catch (JMSException e) {
                        System.out.println("  Error reading properties: " + e.getMessage());
                    }
                } else {
                    System.out.println("Received message of type: " + receivedMessage.getClass().getName());
                }
            }
            
            System.out.println("Received a total of " + count + " non-persistent messages");
            System.out.println("\\nNon-persistent queue example completed");
            
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