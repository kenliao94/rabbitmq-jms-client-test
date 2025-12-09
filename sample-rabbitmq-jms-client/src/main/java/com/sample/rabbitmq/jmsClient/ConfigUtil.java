package com.sample.rabbitmq.jmsClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Properties;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

/**
 * Utility class for reading configuration from .env file
 */
public class ConfigUtil {
    private static Properties envProps;
    
    static {
        loadEnvFile();
    }
    
    private static void loadEnvFile() {
        envProps = new Properties();
        try {
            Files.lines(Paths.get(".env"))
                .filter(line -> line.contains("=") && !line.startsWith("#"))
                .forEach(line -> {
                    String[] parts = line.split("=", 2);
                    envProps.setProperty(parts[0].trim(), parts[1].trim());
                });
        } catch (IOException e) {
            System.err.println("Warning: Could not load .env file: " + e.getMessage());
        }
    }
    
    public static RMQConnectionFactory createConnectionFactory() throws NoSuchAlgorithmException {
        RMQConnectionFactory factory = new RMQConnectionFactory();
        factory.setHost(envProps.getProperty("RABBITMQ_HOST", "localhost"));
        factory.setPort(Integer.parseInt(envProps.getProperty("RABBITMQ_PORT", "5672")));
        factory.setUsername(envProps.getProperty("RABBITMQ_USERNAME", "guest"));
        factory.setPassword(envProps.getProperty("RABBITMQ_PASSWORD", "guest"));
        factory.setVirtualHost(envProps.getProperty("RABBITMQ_VIRTUAL_HOST", "/"));
        if (!Objects.equals(envProps.getProperty("RABBITMQ_HOST", "localhost"), "localhost")) {
            factory.useSslProtocol();
        }
        return factory;
    }
    
    private ConfigUtil() {
        // Prevent instantiation
    }
}