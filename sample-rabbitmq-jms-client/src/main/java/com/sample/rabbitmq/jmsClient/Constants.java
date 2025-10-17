package com.sample.rabbitmq.jmsClient;

/**
 * Common constants used across RabbitMQ JMS examples
 */
public class Constants {
    // RabbitMQ connection parameters
    public static final String RABBITMQ_HOST = "b-983b0a03-6790-47e4-a69e-6573bf894696.mq.us-east-1.on.aws";
    public static final int RABBITMQ_PORT = 5671;
    public static final String RABBITMQ_USERNAME = "admin";
    public static final String RABBITMQ_PASSWORD = "admintestrabbit";
    public static final String RABBITMQ_VIRTUAL_HOST = "/";
    
    // Queue and topic names
    public static final String QUEUE_NAME = "sample.queue";
    public static final String TOPIC_NAME = "sample.topic";
    
    private Constants() {
        // Prevent instantiation
    }
}