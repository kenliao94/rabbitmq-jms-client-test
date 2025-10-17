package com.sample.rabbitmq.jmsClient;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Queue;
import javax.jms.Message;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates JMS 2.0 delivery delay feature
 */
public class DeliveryDelayTestcase {
    
    public static void run() throws Exception {
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setHost(Constants.RABBITMQ_HOST);
        connectionFactory.setPort(Constants.RABBITMQ_PORT);
        connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
        connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
        connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
        connectionFactory.useSslProtocol();
        
        System.out.println("\n=== JMS 2.0 Delivery Delay Testcase ===");
        
        try (JMSContext context = connectionFactory.createContext()) {
            Queue queue = context.createQueue(Constants.QUEUE_NAME + ".delay");
            JMSProducer producer = context.createProducer();
            
            // Send immediate message
            producer.send(queue, "Immediate message");
            System.out.println("Sent immediate message at: " + System.currentTimeMillis());
            
            // Send delayed message (JMS 2.0 feature)
            producer.setDeliveryDelay(3000) // 3 second delay
                   .send(queue, "Delayed message (3 seconds)");
            System.out.println("Sent delayed message at: " + System.currentTimeMillis());
            
            // Send another delayed message
            producer.setDeliveryDelay(5000) // 5 second delay
                   .send(queue, "Delayed message (5 seconds)");
            System.out.println("Sent delayed message at: " + System.currentTimeMillis());
            
            // Create consumer
            JMSConsumer consumer = context.createConsumer(queue);
            
            System.out.println("\nReceiving messages (note timing):");
            
            // Receive messages and show timing
            for (int i = 1; i <= 3; i++) {
                Message message = consumer.receive(10000); // 10 second timeout
                if (message != null) {
                    System.out.println("Received at " + System.currentTimeMillis() + 
                                     ": " + message.getBody(String.class));
                } else {
                    System.out.println("No message received within timeout");
                    break;
                }
            }
            
            consumer.close();
        }
        
        System.out.println("Delivery delay testcase completed");
    }
}