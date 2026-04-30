import pyodbc
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from datetime import datetime, timedelta

# --------------------------
# SQL Server Connection
# --------------------------
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-G0J3UIU;'
    'DATABASE=kafka_data;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# --------------------------
# Kafka / Schema Registry Config
# --------------------------
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'KW3HHT7XAFE7FHBE',
    'sasl.password': 'cfltoaO1qt5yPU7hLG56iwBqFasfQ79kuQaQjw7HOwxwRXto/5pHj/Hhw98osfXw'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-1dx7926.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format(
        '7U7CPJIPQULCKCPC',
        'cflt7j6HxFi97/nwJG6rnhOKKyxnGmAw9vhDge05B256qUwE0pSJmypUjzirk0Lw'
    )
})

# --------------------------
# Delivery report
# --------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# --------------------------
# Track last read timestamp
# --------------------------
try:
    with open("last_read_timestamp.txt", "r") as f:
        last_read_ts = datetime.fromisoformat(f.read().strip())
except FileNotFoundError:
    last_read_ts = datetime.now() - timedelta(days=1)  # default: last 1 day

# --------------------------
# Avro serialization
# --------------------------
def product_to_avro(row, ctx):
    """Convert SQL row to Avro dict"""
    return {
        "id": int(row["id"]),
        "name": str(row["name"]),
        "category": str(row["category"]),
        "price": float(row["price"]),
        "last_updated": row["last_updated"].isoformat()  # convert datetime to string for Kafka
    }

subject_name = 'mysql_data_dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registry:")
print(schema_str)
print("=====================")

key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str, product_to_avro)

producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

# --------------------------
# Fetch new product records
# --------------------------
query = """
SELECT id, name, category, price, last_updated
FROM product
WHERE last_updated >= ?
ORDER BY last_updated ASC, id ASC
"""
cursor.execute(query, (last_read_ts - timedelta(milliseconds=1),))
columns = [column[0] for column in cursor.description]
rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

print("Last read timestamp:", last_read_ts)
print("Number of rows fetched:", len(rows))

if rows:
    for row in rows:
        key = str(row['id'])
        producer.produce(
            topic='mysql_data_dev',
            key=key,
            value=row,
            on_delivery=delivery_report
        )
        print(f"Producing Product ID: {row['id']}")
        producer.poll(0)  # serve delivery callbacks

    # Flush **once** after all messages
    producer.flush()

    # Update last_read_timestamp.txt with the max last_updated
    latest_ts = max(row["last_updated"] for row in rows)
    with open("last_read_timestamp.txt", "w") as f:
        f.write(latest_ts.isoformat())

print("All new records sent to Kafka successfully!")

# --------------------------
# Clean up
# --------------------------
cursor.close()
conn.close()
