import sys
import json
import time
import random

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'words-topic-spark'

SENTENCES = [
    "spark streaming is powerful",
    "kafka provides real-time data",
    "delta lake ensures acid transactions",
    "python is great for data engineering",
    "big data requires distributed computing",
    "apache spark handles batch and streaming",
    "data lakehouse architecture is modern",
    "learning hadoop and spark is fun"
]


def print_cluster_metadata():
    """
    Demonstrates the 'Discovery' mechanism.
    We connect to the bootstrap server, but we ask for the full Cluster Metadata.
    This prints ALL brokers in the cluster, proving we don't need to hardcode them.
    """
    print(f"🔍 Connecting to Bootstrap Server: {BOOTSTRAP_SERVERS}...")
    
    # The AdminClient uses the same discovery logic as the Producer
    admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    
    try:
        # Request metadata from the cluster (this is the "Handshake")
        metadata = admin_client.list_topics(timeout=10)
    except Exception as e:
        print(f"❌ Critical Error: Could not connect to Kafka. {e}")
        sys.exit(1)

    print(f"\n✅ Cluster Discovery Successful!")
    print(f"   Cluster ID: {metadata.cluster_id}")
    print(f"   Controller Broker ID: {metadata.controller_id}")
    print(f"   ------------------------------------------------")
    print(f"   Discovered Brokers ({len(metadata.brokers)} found):")
    
    # Print every broker the client found
    for broker_id, broker in metadata.brokers.items():
        print(f"   - Broker ID: {broker_id} | Address: {broker.host}:{broker.port}")
    
    print(f"   ------------------------------------------------\n")


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}')


def create_topic_if_not_exists(topic_name):
    """ Creates the Kafka topic if it doesn't exist using AdminClient """
    admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    
    # Check if topic exists
    cluster_metadata = admin_client.list_topics(timeout=5.0)
    if topic_name in cluster_metadata.topics:
        print(f"ℹ️  Topic '{topic_name}' already exists.")
        admin_client.delete_topics([topic_name])
        print(f"✅ Topic '{topic_name}' deleted")

    # Create topic
    print(f"⚠️ Topic '{topic_name}' not found. Creating...")
    new_topics = [NewTopic(topic_name, num_partitions=4, replication_factor=1)]
    
    # Call create_topics to asynchronously create topics
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"✅ Topic '{topic}' created")
        except Exception as e:
            print(f"❌ Failed to create topic '{topic}': {e}")


def run_producer():
    print_cluster_metadata()
    
    # 1. Initialize Producer
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'python-producer',
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 100,  # Batch messages for higher throughput
    }
    producer = Producer(conf)

    # 2. Ensure Topic Exists
    create_topic_if_not_exists(TOPIC_NAME)

    print(f"🚀 Starting Producer to topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # 3. Generate Random Data
            # We send a simple string, but in production, this is often JSON or Avro
            sentence = random.choice(SENTENCES)
            
            # Optionally adding a timestamp or ID to make it structured (if needed later)
            # For your notebook, we pass the raw sentence string as 'value'
            
            # 4. Send Data
            # Note: The key is None (Round-robin partitioning). 
            # If ordering matters, use a specific key (e.g., user_id).
            producer.produce(
                TOPIC_NAME, 
                value=sentence.encode('utf-8'), 
                callback=delivery_report
            )

            # Trigger the delivery report callback
            producer.poll(0)

            # Sleep to simulate real-time traffic (adjust for load testing)
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        # 5. Flush and Close
        # Ensure all buffered messages are sent before exiting
        remaining = producer.flush(10.0) 
        if remaining > 0:
            print(f"⚠️ {remaining} messages were still in queue when flushed.")
        else:
            print("✅ All messages flushed.")