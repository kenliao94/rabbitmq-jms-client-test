package com.sample.rabbitmq.jmsClient;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Topic;
import javax.jms.Message;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates JMS 2.0 shared consumers for load balancing
 */
public class SharedConsumerTestcase {
    
    public static void run() throws Exception {
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setHost(Constants.RABBITMQ_HOST);
        connectionFactory.setPort(Constants.RABBITMQ_PORT);
        connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
        connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
        connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
        connectionFactory.useSslProtocol();
        
        System.out.println("\n=== JMS 2.0 Shared Consumer Testcase ===");
        
        try (JMSContext context = connectionFactory.createContext()) {
            Topic topic = context.createTopic(Constants.TOPIC_NAME + ".shared");
            JMSProducer producer = context.createProducer();
            
            // Send messages
            for (int i = 1; i <= 6; i++) {
                String messageText = "Shared consumer message #" + i;
                producer.send(topic, messageText);
                System.out.println("Sent: " + messageText);
            }
            
            // Create shared consumers (JMS 2.0 feature)
            JMSConsumer consumer1 = context.createSharedConsumer(topic, "SharedSubscription");
            JMSConsumer consumer2 = context.createSharedConsumer(topic, "SharedSubscription");
            
            System.out.println("\nReceiving with shared consumers:");
            
            // Consume messages with both consumers
            for (int i = 1; i <= 6; i++) {
                Message message1 = consumer1.receiveNoWait();
                Message message2 = consumer2.receiveNoWait();
                
                if (message1 != null) {
                    System.out.println("Consumer1 received: " + message1.getBody(String.class));
                }
                if (message2 != null) {
                    System.out.println("Consumer2 received: " + message2.getBody(String.class));
                }
                
                if (message1 == null && message2 == null) {
                    Thread.sleep(100); // Brief pause before next attempt
                }
            }
            
            consumer1.close();
            consumer2.close();
        }
        
        System.out.println("Shared consumer testcase completed");
    }
}