import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# --------------------------
# Kafka / Schema Registry Config
# --------------------------
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'KW3HHT7XAFE7FHBE',
    'sasl.password': 'cfltoaO1qt5yPU7hLG56iwBqFasfQ79kuQaQjw7HOwxwRXto/5pHj/Hhw98osfXw',
    'group.id': 'my_test_group',
    'auto.offset.reset': 'latest'  # read messages from beginning
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-1dx7926.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format(
        '7U7CPJIPQULCKCPC',
        'cflt7j6HxFi97/nwJG6rnhOKKyxnGmAw9vhDge05B256qUwE0pSJmypUjzirk0Lw'
    )
})

# --------------------------
# Fetch latest schema
# --------------------------
subject_name = 'mysql_data_dev-value'
latest_schema = schema_registry_client.get_latest_version(subject_name).schema
schema_str = latest_schema.schema_str

# --------------------------
# Avro Deserializer
# --------------------------
def avro_to_dict(obj, ctx):
    return obj  # Avro → Python dict

avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    avro_to_dict
)

key_deserializer = StringDeserializer('utf_8')

# --------------------------
# Consumer Configuration
# --------------------------
consumer_conf = {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'group.id': 'product_consumer_group_1',
    'auto.offset.reset': 'earliest',
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer
}

consumer = DeserializingConsumer(consumer_conf)

# --------------------------
# Business Transformation Logic
# --------------------------
def transform_record(record: dict) -> dict:
    if not record:
        return record

    # Convert category to uppercase
    category = record.get("category")
    if category:
        category = category.upper()
        record["category"] = category

    # Apply discount rules
    price = record.get("price")
    if price is not None:
        if category == "ELECTRONICS":
            record["price"] = round(price * 0.90, 2)
        elif category == "CLOTHING":
            record["price"] = round(price * 0.95, 2)

    return record

# --------------------------
# File to append JSON records
# --------------------------
OUTPUT_FILE = "transformed_products.json"

# --------------------------
# Subscribe & Consume
# --------------------------
consumer.subscribe(['mysql_data_dev'])

print("Listening for messages on topic 'mysql_data_dev'...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        key = msg.key()
        value = msg.value()

        # Transform record
        transformed_value = transform_record(value)

        # Convert to JSON string
        json_record = json.dumps({
            "product_id": key,
            "data": transformed_value
        })

        # Append JSON string to file
        with open(OUTPUT_FILE, "a") as f:
            f.write(json_record + "\n")

        print(f"Appended record for Product ID={key}")

except KeyboardInterrupt:
    print("Consumer stopped manually.")

finally:
    consumer.close()
