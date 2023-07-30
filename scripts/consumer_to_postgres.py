import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
import config
from flask import Flask, jsonify
import time
import logging
from logging.handlers import RotatingFileHandler

app = Flask(__name__)
# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': config.bootstrap_servers,  # Replace with your Kafka broker address
    'group.id': config.group_id,  # Consumer group ID
    'auto.offset.reset': config.auto_offset_reset  # Start reading from the beginning of the topic
}

# PostgreSQL Connection Configuration
db_config = {
    'host': config.db_host,
    'port': config.db_port,
    'database': config.db_name,
    'user': config.db_username,
    'password': config.db_password
}

msg_counter = 1

logging.basicConfig(filename='consume.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def write_to_postgres(data):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Assuming 'data' is a dictionary representing the JSON data received
    # Modify the following INSERT statement based on your table schema
    insert_query = f"INSERT INTO {config.db_target_table} (column1, column2, ...) VALUES (%s, %s, ...);"
    cursor.execute(insert_query, (data['column1'], data['column2'], ...))

    conn.commit()
    logging.info(f'Data inserted : {data}')
    cursor.close()
    conn.close()


def kafka_consumer_callback(msg):
    try:
        data = json.loads(msg.value())
        write_to_postgres(data)
    except json.JSONDecodeError as e:
        logging.error(f'Failed to parse JSON message: {e}')
    except Exception as e:
        logging.error(f'Failed to write to PostgreSQL: {e}')


@app.route('/health_consumer')
def health_check():
    return jsonify({"consumer status": "healthy"}), 200


# http://localhost:5001/health_consumer

@app.route('/')
def home():
    logging.info('Started Consume!')
    return "Started Consume!"


@app.route('/consume')
def consume():
    global msg_counter
    consumer = Consumer(consumer_conf)
    consumer.subscribe([config.topic])  # Replace 'my_topic' with the Kafka topic you want to consume from

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.error("Reached end of partition")
                else:
                    logging.error(f"Error while consuming: {msg.error()}")
            else:
                print(
                    f"Consumed msg ({msg_counter}) : {str(msg.value().decode('utf-8'))} || delivered from {config.topic} [{msg.partition()}]")
                logging.info(
                    f"Consumed msg ({msg_counter}) : {str(msg.value().decode('utf-8'))} || delivered from {config.topic} [{msg.partition()}]")
                msg_counter = msg_counter + 1
                kafka_consumer_callback(msg)


    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


# http://localhost:5001/consume

if __name__ == "__main__":
    handler = RotatingFileHandler('consume.log', maxBytes=1024 * 1024, backupCount=3)
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)

    app.run(debug=True, port=5001)
