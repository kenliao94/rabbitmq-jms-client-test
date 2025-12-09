package com.sample.rabbitmq.jmsClient;

/**
 * RabbitMQ JMS Client Sample Application
 * 
 * This sample demonstrates how to:
 * 1. Connect to a local RabbitMQ broker
 * 2. Send messages to a queue
 * 3. Receive messages from a queue
 * 4. Work with topics for publish/subscribe
 * 5. Use JMS transactions
 * 6. Send non-persistent messages
 */
public class App {
    
    public static void main(String[] args) {
        System.out.println("RabbitMQ JMS Client Sample Starting...");
        
        try {
            PublishToAMQPQueueTest.run();
//            ConsumeFromAMQPQueueTest.run();
//            ConsumeFromAMQPStreamTest.run();
        } catch (Exception e) {
            System.err.println("Error in sample: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
