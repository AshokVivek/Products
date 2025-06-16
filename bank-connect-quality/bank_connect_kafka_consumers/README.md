
# Bank Connect Kafka Consumers

This repository contains all the Kafka consumers needed for **BankConnect**, organized and containerized for efficient management and scalability. Kafka consumers are written in Python, and Docker is used for containerization. Each consumer listens to a Kafka topic and processes messages, with flexible deployment options and scaling capabilities.

In case of any queries, contact `Ganesh Prasad <ganesh.prasad@finbox.in>`

## Table of Contents

1. [Overview](#overview)
2. [Folder Structure](#folder-structure)
3. [Rules to Follow](#rules-to-follow)
4. [Configuration](#configuration)
    - [conf.py](#confpy)
    - [consumer_startup.py](#consumer-startuppy)
    - [deployment_details.json](#deployment_detailsjson)
    - [Dockerfiles](#dockerfiles)
5. [Running the Consumers](#running-the-consumers)
    - [Shell Script Usage](#shell-script-usage)
6. [Managing Kafka Consumers](#managing-kafka-consumers)
    - [Kafka Admin](#kafka-admin)
    - [Kafka Consumer Manager](#kafka-consumer-manager)
7. [Producer](#producer)
8. [Scaling Consumers](#scaling-consumers)

---

## Overview

This repository is responsible for managing Kafka consumers within **BankConnect**. Consumers are designed to be lightweight and can be easily deployed and scaled using Docker. The repository contains configuration files, utility scripts, and a `deployment_details.json` file for managing the consumers.

## Folder Structure
```bash
bank-connect-kafka-consumers
│
├── consumers/
│   ├── __init__.py
│   ├── email_delivering_consumers.py
│   ├── inconsistency_solve_consumer.py
│   ├── quality_events_consumer.py
│   └── recurring_bulk_pull_consumer.py
│
├── conf.py
├── consumer_startup.py
├── deployment_details.json
├── Dockerfile.kafka.dashboard
├── Dockerfile.kafka.quality
├── kafka_admin.py
├── kafka_consumer_manager.py
├── producer.py
└── README.md
```

---

## Rules to Follow

1. **Adding a New Consumer**:
   - Add a new consumer by creating a `.py` file inside the `consumers/` directory.
   - The **file name** and the **function name** within the file must be **exclusive** and should be **the same**.

2. **Deployment Details**:
   - Update the `deployment_details.json` file with all relevant details for each consumer to be deployed.
   
3. **FinboxDashboard Consumers**:
   - When writing consumers for `FinboxDashboard`, ensure you include the following commands at the beginning of the file to properly set up Django:
     \`\`\`python
     import django
     django.setup()
     \`\`\`

---

## Configuration

### `conf.py`

This file contains configuration settings for the Kafka consumers. It is where you would define any global or default values for the consumers, such as Kafka broker URLs, group IDs, and other environment settings. Ensure that this file is updated to reflect any environment-specific configurations.

### `consumer_startup.py`

This script is responsible for starting up the Kafka consumers. It contains initialization logic that may be shared across all consumers. It ensures that each consumer is set up with the necessary Kafka configurations and that Django is properly initialized for those related to `FinboxDashboard`.

### `deployment_details.json`

This file holds the configuration details for each Kafka consumer. Each consumer has its own set of configurations, including the topic name, group ID, number of workers, and other consumer-specific settings.

Example configuration from `deployment_details.json`:

\`\`\`json
{
  "consumers": {
    "recurring_bulk_pull_consumers": {
      "ENABLE": true,
      "TOPIC_NAME": "recurring_bulk_pull",
      "GROUP_ID": "recurring_bulk_pull_consumers_group",
      "NUMBER_OF_WORKERS": 2,
      "NUMBER_OF_THREADS": 2,
      "CONSUMER_FUNCTION_NAME": "recurring_bulk_pull_consumer",
      "IS_BATCH_CONSUMPTION_ENABLED": false,
      "MESSAGE_CONSUMPTION_BATCH_SIZE": 1,
      "MAX_POLL_INTERVAL_IN_MILLISECONDS": 600000
    }
  }
}
\`\`\`

This JSON structure ensures that each consumer is configured correctly for deployment.

#### Make sure to add your consumer configuration here, so that deployment is taken care of.

### Dockerfiles

- **`Dockerfile.kafka.dashboard`**: This Dockerfile is used to build and deploy consumers specific to the `FinboxDashboard`. It ensures that all necessary dependencies and configurations are in place for consumers related to the dashboard.
  
- **`Dockerfile.kafka.quality`**: This Dockerfile is used to build consumers that handle quality Tool. It is tailored to ensure the correct environment for processing Kafka messages related to quality Tool.

`Note : Due to this coupling, you can now reuse the code of both FinboxDashboard and Quality Tool in your conusmers by directly importing them.` 

---

## Running the Consumers

### Shell Script Usage

Follow the usual way of deploying FinboxDashboard or Quality tool and the rest is taken care of.

---

## Managing Kafka Consumers

### Kafka Admin

`kafka_admin.py` is a script used to manage Kafka topics, partitions, and other administrative tasks. It can be used to create new Kafka topics or modify existing ones. Ensure this script is used for setting up Kafka topics before deploying consumers.

### Kafka Consumer Manager

`kafka_consumer_manager.py` is responsible for managing the lifecycle of Kafka consumers, including starting, stopping, and scaling consumers. This script provides utilities to ensure that consumers are running properly.

---

## Producer

`producer.py` handles the production of Kafka messages. It is useful for generating Kafka messages. This script allows you to produce messages into Kafka topics that consumers will then process. You can create an obkect of this class and use the `send()` method to produce message into the desired kafka topic.

---

## Scaling Consumers

Work in progress

---


