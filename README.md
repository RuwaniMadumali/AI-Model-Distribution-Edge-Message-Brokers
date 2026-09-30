# AI Model Distribution Using Edge Message Brokers

This repository contains the experimental implementation for evaluating
Apache Kafka and RabbitMQ as message brokers for AI model distribution
in edge computing environments.

The project compares the performance and reliability of two model-distribution
approaches under different model sizes, simulated device counts, and network
conditions.

## Research Objective

The main objective is to evaluate how Apache Kafka and RabbitMQ perform when
distributing AI models to simulated edge devices.

The evaluation focuses on:

- End-to-end latency
- Delivery success
- Reliability under constrained network conditions
- The effect of increasing model size
- The effect of increasing the number of edge devices
- Full-model distribution compared with metadata-based distribution

## Distribution Approaches

### Full Model Distribution

The complete model binary file is transmitted through the message broker
to the consumer.

Three model sizes are used:

- 10 MB
- 50 MB
- 100 MB

### Metadata-Based Distribution

A small metadata message is transmitted through the message broker instead
of the complete model file.

The metadata contains information such as the model filename, version,
size, and download location.

After receiving the metadata, the consumer downloads the corresponding
model file from a local HTTP server.

## Repository Structure

```text
AI-Model-Distribution-Edge-Message-Brokers/
├── kafka/
│   ├── kafka_consumer_full.py
│   ├── kafka_consumer_meta.py
│   ├── kafka_producer_full.py
│   ├── kafka_producer_meta.py
│   └── run_kafka_experiments.py
├── rabbitmq/
│   ├── consumer_full.py
│   ├── consumer_metadata.py
│   ├── experiments.py
│   ├── producer_full.py
│   └── producer_metadata.py
├── .gitignore
├── LICENSE
└── README.md
```

Large model binaries, received model files, virtual environments, and
temporary experiment files are excluded from the repository.

---

# How to Run the Experiments

Kafka and RabbitMQ are implemented as **separate experimental setups**.

Run each experiment from its corresponding project folder.

## Prerequisites

Make sure the following are installed:

- Python
- Docker
- Git

Install the required Python libraries:

```bash
pip install pika kafka-python pandas matplotlib requests
```

---

# RabbitMQ Setup

## 1. Start RabbitMQ

Pull the RabbitMQ Docker image:

```bash
docker pull rabbitmq:3-management
```

Create and start the RabbitMQ container:

```bash
docker run -d --name rabbitmq --cap-add=NET_ADMIN --cap-add=NET_RAW -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Check that the container is running:

```bash
docker ps
```

RabbitMQ will be available locally on port `5672`.

The RabbitMQ Management interface is available on port `15672`.

## 2. Create the Test Model Files

Navigate to the `rabbitmq` project folder and run:

```bash
python -c "import os; [open(f'model_{s}MB.bin', 'wb').write(os.urandom(s*1024*1024)) for s in (10, 50, 100)]"
```

This creates three dummy binary files inside the RabbitMQ project folder:

```text
model_10MB.bin
model_50MB.bin
model_100MB.bin
```

These files represent AI model payloads of 10 MB, 50 MB, and 100 MB.
They contain randomly generated binary data and do not represent actual
trained AI models.

## 3. Start the Local HTTP Server

The local HTTP server is required for the **metadata-based distribution**
experiments.

Navigate to the folder containing the model files and run:

```bash
python -m http.server 8000
```

Keep this terminal running while performing the metadata-based experiments.

## 4. Run the RabbitMQ Experiments

Open another terminal and navigate to the `rabbitmq` project folder.

Run the experiment runner:

```bash
python experiments.py
```

The experiment runner executes the configured RabbitMQ experiments using
the specified model sizes and experimental conditions.

---

# Kafka Setup

## 1. Start ZooKeeper

Run ZooKeeper using Docker:

```bash
docker run -d --name zookeeper -p 2181:2181 zookeeper
```

## 2. Create and Start Kafka

Create the Kafka container:

```bash
docker run -d --name kafka -p 9092:9092 \
-e KAFKA_PROCESS_ROLES=broker,controller \
-e KAFKA_NODE_ID=1 \
-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
confluentinc/cp-kafka
```

If the Kafka container has already been created, it can be started using:

```bash
docker start kafka
```

Check that the container is running:

```bash
docker ps
```

## 3. Create the Test Model Files

Navigate to the `kafka` project folder and run:

```bash
python -c "import os; [open(f'model_{s}MB.bin', 'wb').write(os.urandom(s*1024*1024)) for s in (10, 50, 100)]"
```

This creates three dummy binary files inside the RabbitMQ project folder:

```text
model_10MB.bin
model_50MB.bin
model_100MB.bin
```

These files represent AI model payloads of 10 MB, 50 MB, and 100 MB.
They contain randomly generated binary data and do not represent actual
trained AI models.
## 4. Start the Local HTTP Server

The HTTP server is required for the **metadata-based distribution**
experiments.

From the folder containing the model files, run:

```bash
python -m http.server 8000
```

Keep this terminal running during the metadata-based experiments.

## 5. Run the Kafka Experiments

Open another terminal and navigate to the `kafka` project folder.

Run:

```bash
python run_kafka_experiments_v2.py
```

The experiment runner executes the configured Kafka experiments using
the specified model sizes and experimental conditions.

---

# Experiment Execution

The Kafka and RabbitMQ experiments are executed **separately**.

The general experimental procedure is:

```text
Start Message Broker Using Docker
              ↓
Create Model Payloads
     10 MB, 50 MB, 100 MB
              ↓
Start Local HTTP Server
(for metadata-based transmission)
              ↓
Run Experiment Script
              ↓
Collect Experimental Measurements
```

The local HTTP server is required only for metadata-based distribution,
where the message broker transfers model metadata and the consumer retrieves
the actual model file separately.

For full-model distribution, the complete binary model payload is transmitted
through the message broker.

## Note

The `.bin` files used in the experiments are dummy payloads created to
represent AI models of different sizes. They do not contain actual trained
machine-learning models.

Kafka and RabbitMQ should be tested separately using their corresponding
experiment runners.
