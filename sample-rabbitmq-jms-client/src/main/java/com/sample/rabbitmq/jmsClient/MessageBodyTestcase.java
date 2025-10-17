package com.sample.rabbitmq.jmsClient;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Queue;
import javax.jms.Message;
import java.util.Map;
import java.util.HashMap;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Demonstrates JMS 2.0 simplified message body handling
 */
public class MessageBodyTestcase {
    
    public static void run() throws Exception {
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setHost(Constants.RABBITMQ_HOST);
        connectionFactory.setPort(Constants.RABBITMQ_PORT);
        connectionFactory.setUsername(Constants.RABBITMQ_USERNAME);
        connectionFactory.setPassword(Constants.RABBITMQ_PASSWORD);
        connectionFactory.setVirtualHost(Constants.RABBITMQ_VIRTUAL_HOST);
        connectionFactory.useSslProtocol();
        
        System.out.println("\n=== JMS 2.0 Message Body Testcase ===");
        
        try (JMSContext context = connectionFactory.createContext()) {
            Queue queue = context.createQueue(Constants.QUEUE_NAME + ".body");
            JMSProducer producer = context.createProducer();
            JMSConsumer consumer = context.createConsumer(queue);
            
            // Send different types of message bodies (JMS 2.0 simplified API)
            
            // String message
            producer.send(queue, "Simple string message");
            System.out.println("Sent string message");
            
            // Map message
            Map<String, Object> mapData = new HashMap<>();
            mapData.put("name", "John Doe");
            mapData.put("age", 30);
            mapData.put("active", true);
            producer.send(queue, mapData);
            System.out.println("Sent map message");
            
            // Byte array message
            byte[] byteData = "Binary data content".getBytes();
            producer.send(queue, byteData);
            System.out.println("Sent byte array message");
            
            System.out.println("\nReceiving messages with body type handling:");
            
            // Receive and handle different message types
            for (int i = 1; i <= 3; i++) {
                Message message = consumer.receive(2000);
                if (message != null) {
                    System.out.println("Message " + i + ":");
                    
                    // JMS 2.0 simplified body access
                    if (message.isBodyAssignableTo(String.class)) {
                        String body = message.getBody(String.class);
                        System.out.println("  String body: " + body);
                    } else if (message.isBodyAssignableTo(Map.class)) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> body = message.getBody(Map.class);
                        System.out.println("  Map body: " + body);
                    } else if (message.isBodyAssignableTo(byte[].class)) {
                        byte[] body = message.getBody(byte[].class);
                        System.out.println("  Byte array body: " + new String(body));
                    } else {
                        System.out.println("  Unknown body type");
                    }
                } else {
                    System.out.println("No message received within timeout");
                    break;
                }
            }
            
            consumer.close();
        }
        
        System.out.println("Message body testcase completed");
    }
}