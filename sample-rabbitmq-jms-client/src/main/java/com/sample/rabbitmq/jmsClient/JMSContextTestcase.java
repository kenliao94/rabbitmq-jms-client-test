package com.sample.rabbitmq.jmsClient;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Queue;
import javax.jms.Message;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates JMS 2.0 JMSContext API (simplified resource management)
 */
public class JMSContextTestcase {
    
    public static void run() throws Exception {
        // Create a connection factory
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setHost(Constants.RABBITMQ_HOST);
        connectionFactory.setPort(Constants.RABBITMQ_PORT);
        connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
        connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
        connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
        connectionFactory.useSslProtocol();
        
        System.out.println("\n=== JMS 2.0 Context API Testcase ===");
        
        // Use try-with-resources for automatic resource management
        try (JMSContext context = connectionFactory.createContext()) {
            
            // Create a queue
            Queue queue = context.createQueue(Constants.QUEUE_NAME + ".jms2");
            
            // Create a producer (JMS 2.0 style)
            JMSProducer producer = context.createProducer();
            
            // Send messages using method chaining (JMS 2.0 feature)
            for (int i = 1; i <= 3; i++) {
                String messageText = "JMS 2.0 Context message #" + i;
                producer.setProperty("MessageNumber", i)
                       .setProperty("API", "JMS 2.0")
                       .send(queue, messageText);
                System.out.println("Sent: " + messageText);
            }
            
            // Create a consumer (JMS 2.0 style)
            JMSConsumer consumer = context.createConsumer(queue);
            
            // Receive messages
            System.out.println("\nReceiving messages:");
            for (int i = 1; i <= 3; i++) {
                Message message = consumer.receive(2000);
                if (message != null) {
                    System.out.println("Received: " + message.getBody(String.class));
                    System.out.println("  MessageNumber: " + message.getIntProperty("MessageNumber"));
                    System.out.println("  API: " + message.getStringProperty("API"));
                } else {
                    System.out.println("No message received within timeout");
                    break;
                }
            }
            
            consumer.close();
        } // JMSContext automatically closed here
        
        System.out.println("JMS 2.0 Context API testcase completed");
    }
}