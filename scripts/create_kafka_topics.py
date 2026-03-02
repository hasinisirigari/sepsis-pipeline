# Creating Kafka topics for the sepsis pipeline.

from confluent_kafka.admin import AdminClient, NewTopic
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Three topics matching the data streams
topics = [
    NewTopic("patient.vitals", num_partitions=3, replication_factor=1),
    NewTopic("patient.labs", num_partitions=3, replication_factor=1),
    NewTopic("patient.medications", num_partitions=3, replication_factor=1),
]

# Creating topics - idempotent (won't fail if they already exist)
futures = admin.create_topics(topics)

for topic, future in futures.items():
    try:
        future.result()
        print(f"Created topic: {topic}")
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            print(f"Topic already exists: {topic}")
        else:
            print(f"Error creating {topic}: {e}")

# Verify
metadata = admin.list_topics()
print(f"\nAll topics on broker:")
for topic in sorted(metadata.topics):
    if not topic.startswith("_"):  # skip internal topics
        print(f"  {topic} ({len(metadata.topics[topic].partitions)} partitions)")
