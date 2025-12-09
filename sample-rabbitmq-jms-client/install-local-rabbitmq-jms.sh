#!/bin/bash

# Copy the JAR to current directory
cp /Users/qrl/amzn/personal_workspace/rabbitmq-jms-client/target/rabbitmq-jms-3.5.0-SNAPSHOT.jar .

# Install to local Maven repository with POM
mvn install:install-file \
  -Dfile=rabbitmq-jms-3.5.0-SNAPSHOT.jar \
  -DpomFile=/Users/qrl/amzn/personal_workspace/rabbitmq-jms-client/pom.xml
