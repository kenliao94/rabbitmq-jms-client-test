package com.sample.rabbitmq.jmsClient;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.rabbitmq.jms.admin.RMQConnectionFactory;
import com.rabbitmq.jms.admin.RMQDestination;

/**
 * RabbitMQ JMS Client Sample Application
 * 
 * This sample demonstrates how to:
 * 1. Connect to a local RabbitMQ broker
 * 2. Send messages to a queue
 * 3. Receive messages from a queue
 * 4. Work with topics for publish/subscribe
 */
public class App {
    // RabbitMQ connection parameters
    private static final String RABBITMQ_HOST = "localhost";
    private static final int RABBITMQ_PORT = 5672;
    private static final String RABBITMQ_USERNAME = "guest";
    private static final String RABBITMQ_PASSWORD = "guest";
    private static final String RABBITMQ_VIRTUAL_HOST = "/";
    
    // Queue and topic names
    private static final String QUEUE_NAME = "sample.queue";
    private static final String TOPIC_NAME = "sample.topic";
    
    public static void main(String[] args) {
        System.out.println("RabbitMQ JMS Client Sample Starting...");
        
        try {
//            // Run the queue example (point-to-point messaging)
            queueExample();
//            // Run the topic example (publish/subscribe messaging)
            topicExample();
            // Run the transaction example
            transactionExample();
            // Run the non-persistent queue example
            nonPersistentQueueExample();
            
            System.out.println("Sample completed successfully!");
        } catch (Exception e) {
            System.err.println("Error in sample: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Demonstrates point-to-point messaging using a queue
     */
    private static void queueExample() throws Exception {
        Connection connection = null;
        
        try {
            // Create a connection factory
            RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
            connectionFactory.setHost(RABBITMQ_HOST);
            connectionFactory.setPort(RABBITMQ_PORT);
            connectionFactory.setUsername(RABBITMQ_USERNAME);
            connectionFactory.setPassword(RABBITMQ_PASSWORD);
            connectionFactory.setVirtualHost(RABBITMQ_VIRTUAL_HOST);
            
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
            Queue queue = session.createQueue(QUEUE_NAME);
            
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

    /**
     * Demonstrates JMS transactions for reliable messaging
     */
    private static void transactionExample() throws Exception {
        Connection connection = null;
        
        try {
            // Create a connection factory
            RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
            connectionFactory.setHost(RABBITMQ_HOST);
            connectionFactory.setPort(RABBITMQ_PORT);
            connectionFactory.setUsername(RABBITMQ_USERNAME);
            connectionFactory.setPassword(RABBITMQ_PASSWORD);
            connectionFactory.setVirtualHost(RABBITMQ_VIRTUAL_HOST);
            
            // Create a connection
            connection = connectionFactory.createConnection();
            connection.start();
            
            // Create a transacted session (first parameter true means transacted)
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            
            // Create a queue destination for transactions
            Queue queue = session.createQueue(QUEUE_NAME + ".transactions");
            
            // Create a message producer
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            System.out.println("\n=== Transaction Example ===");
            
            // First transaction - will be committed
            System.out.println("\nStarting first transaction (will commit):");
            for (int i = 1; i <= 3; i++) {
                String messageText = "Tx1-Message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                producer.send(message);
                System.out.println("Added to transaction: " + messageText);
            }
            
            // Commit the transaction - all messages will be sent atomically
            session.commit();
            System.out.println("First transaction committed successfully");
            
            // Second transaction - will be rolled back
            System.out.println("\nStarting second transaction (will rollback):");
            for (int i = 1; i <= 3; i++) {
                String messageText = "Tx2-Message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                producer.send(message);
                System.out.println("Added to transaction: " + messageText);
            }
            
            // Rollback the transaction - no messages will be sent
            session.rollback();
            System.out.println("Second transaction rolled back - no messages were sent");
            
            // Third transaction - will be committed
            System.out.println("\nStarting third transaction (will commit):");
            for (int i = 1; i <= 2; i++) {
                String messageText = "Tx3-Message #" + i;
                TextMessage message = session.createTextMessage(messageText);
                producer.send(message);
                System.out.println("Added to transaction: " + messageText);
            }
            
            // Commit the transaction
            session.commit();
            System.out.println("Third transaction committed successfully");
            
            // Now consume the messages that were successfully committed
            System.out.println("\nReceiving committed messages:");
            
            // Create a transacted consumer session
            Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            
            // We should receive 5 messages (3 from first transaction + 2 from third transaction)
            int expectedMessages = 5;
            int receivedCount = 0;
            
            while (receivedCount < expectedMessages) {
                Message receivedMessage = consumer.receive(2000); // 2 second timeout
                
                if (receivedMessage == null) {
                    System.out.println("No more messages received within timeout period");
                    break;
                }
                
                if (receivedMessage instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) receivedMessage;
                    System.out.println("Received: " + textMessage.getText());
                    receivedCount++;
                    
                    // Simulate processing every other message successfully
                    if (receivedCount % 2 == 0) {
                        // Commit after every other message
                        consumerSession.commit();
                        System.out.println("  - Processing committed");
                    } else {
                        // For demonstration, rollback odd-numbered messages
                        consumerSession.rollback();
                        System.out.println("  - Processing rolled back (message returns to queue)");
                        // The message will be redelivered
                    }
                }
            }
            
            // Final commit to acknowledge any pending messages
            consumerSession.commit();
            
            // Clean up resources
            consumer.close();
            producer.close();
            consumerSession.close();
            session.close();
            
            System.out.println("\nTransaction example completed");
            
        } finally {
            // Always close the connection
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    /**
     * Demonstrates sending non-persistent messages to a queue
     */
    private static void nonPersistentQueueExample() throws Exception {
        Connection connection = null;
        
        try {
            // Create a connection factory
            RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
            connectionFactory.setHost(RABBITMQ_HOST);
            connectionFactory.setPort(RABBITMQ_PORT);
            connectionFactory.setUsername(RABBITMQ_USERNAME);
            connectionFactory.setPassword(RABBITMQ_PASSWORD);
            connectionFactory.setVirtualHost(RABBITMQ_VIRTUAL_HOST);
            
            // Create a connection
            connection = connectionFactory.createConnection();
            connection.start();
            
            // Create a non-transacted, auto-acknowledged session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create a queue destination for non-persistent messages
            Queue queue = session.createQueue(QUEUE_NAME + ".nonpersistent");
            
            // Create a message producer with NON_PERSISTENT delivery mode
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // Explicitly set to non-persistent
            
            System.out.println("\n=== Non-Persistent Queue Example ===");
            
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
            System.out.println("\nReceiving non-persistent messages:");
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
            System.out.println("\nNon-persistent queue example completed");
            
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

    /**
     * Demonstrates publish/subscribe messaging using a topic
     */
    private static void topicExample() throws Exception {
        Connection connection = null;
        
        try {
            // Create a connection factory
            RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
            connectionFactory.setHost(RABBITMQ_HOST);
            connectionFactory.setPort(RABBITMQ_PORT);
            connectionFactory.setUsername(RABBITMQ_USERNAME);
            connectionFactory.setPassword(RABBITMQ_PASSWORD);
            connectionFactory.setVirtualHost(RABBITMQ_VIRTUAL_HOST);
            
            // Create a connection
            connection = connectionFactory.createConnection();
            connection.start();
            
            // Create a non-transacted, auto-acknowledged session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create a topic destination
            Topic topic = session.createTopic(TOPIC_NAME);
            
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
