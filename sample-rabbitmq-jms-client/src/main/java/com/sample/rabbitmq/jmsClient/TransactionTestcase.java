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
 * Demonstrates JMS transactions for reliable messaging
 */
public class TransactionTestcase {
    
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
            
            // Create a transacted session (first parameter true means transacted)
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            
            // Create a queue destination for transactions
            Queue queue = session.createQueue(Constants.QUEUE_NAME + ".transactions");
            
            // Create a message producer
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            System.out.println("\\n=== Transaction Example ===");
            
            // First transaction - will be committed
            System.out.println("\\nStarting first transaction (will commit):");
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
            System.out.println("\\nStarting second transaction (will rollback):");
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
            System.out.println("\\nStarting third transaction (will commit):");
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
            System.out.println("\\nReceiving committed messages:");
            
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
            
            System.out.println("\\nTransaction example completed");
            
        } finally {
            // Always close the connection
            if (connection != null) {
                connection.close();
            }
        }
    }
}