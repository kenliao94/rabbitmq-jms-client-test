package com.sample.rabbitmq.jmsClient;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Queue;
import javax.jms.Message;
import javax.jms.CompletionListener;
import javax.jms.JMSException;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates JMS 2.0 asynchronous message sending with CompletionListener
 */
public class AsyncConsumerTestcase {
    
    public static void run() throws Exception {
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setHost(Constants.RABBITMQ_HOST);
        connectionFactory.setPort(Constants.RABBITMQ_PORT);
        connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
        connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
        connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
        connectionFactory.useSslProtocol();
        
        System.out.println("\n=== JMS 2.0 Async Send Testcase ===");
        
        try (JMSContext context = connectionFactory.createContext()) {
            Queue queue = context.createQueue(Constants.QUEUE_NAME + ".async");
            JMSProducer producer = context.createProducer();
            
            // Send messages asynchronously with completion listener
            for (int i = 1; i <= 3; i++) {
                final int messageNum = i;
                String messageText = "Async message #" + i;
                
                producer.setAsync(new CompletionListener() {
                    @Override
                    public void onCompletion(Message message) {
                        try {
                            System.out.println("Message " + messageNum + " sent successfully: " + 
                                             message.getBody(String.class));
                        } catch (JMSException e) {
                            System.err.println("Error in completion callback: " + e.getMessage());
                        }
                    }
                    
                    @Override
                    public void onException(Message message, Exception exception) {
                        System.err.println("Failed to send message " + messageNum + ": " + 
                                         exception.getMessage());
                    }
                });
                
                producer.send(queue, messageText);
                System.out.println("Initiated async send for: " + messageText);
            }
            
            // Wait for async operations to complete
            Thread.sleep(2000);
            
            // Consume messages
            JMSConsumer consumer = context.createConsumer(queue);
            System.out.println("\nReceiving async messages:");
            
            for (int i = 1; i <= 3; i++) {
                Message message = consumer.receive(2000);
                if (message != null) {
                    System.out.println("Received: " + message.getBody(String.class));
                } else {
                    System.out.println("No message received within timeout");
                    break;
                }
            }
            
            consumer.close();
        }
        
        System.out.println("Async send testcase completed");
    }
}