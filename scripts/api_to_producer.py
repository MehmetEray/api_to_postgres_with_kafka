from flask import Flask, jsonify
import requests
import time
from confluent_kafka import Producer
import config
import json
import logging
from logging.handlers import RotatingFileHandler

app = Flask(__name__)

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': config.bootstrap_servers,  # Replace with your Kafka broker address
}

logging.basicConfig(filename='produce.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

msg_counter = 1


def fetch_json_data_from_api_continuously():
    while True:
        try:
            response = requests.get(config.api_base_url)
            if response.status_code == 200:
                data = response.json()
                print(data)
                return data
            else:
                logging.error(
                    f"Failed to fetch data from API. Status code: {response.status_code}. Response: {response}")
                raise Exception(
                    f"Failed to fetch data from API. Status code: {response.status_code}. Response: {response}")


        except Exception as e:
            logging.error(f'Exception: {e}')

        time.sleep(0.1)  # wait x seconds and send request again


def kafka_delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
        logging.error(f"Failed to deliver message: {err}")
    else:
        pass
        # logging.error(f"Produced msg : {msg.value().decode('utf-8')} || delivered to {msg.topic()} [{msg.partition()}]")


@app.route('/health_producer')
def health_check():
    return jsonify({"producer status": "healthy"}), 200


# http://localhost:5000/health_producer


@app.route('/')
def home():
    logging.info("Started Produce!")
    return "Started Produce!"


@app.route('/produce')
def produce():
    global msg_counter
    producer = Producer(producer_conf)
    try:
        while True:
            data = fetch_json_data_from_api_continuously()
            data_str = json.dumps(data)  # Convert data to JSON string format
            producer.produce(config.topic, value=data_str.encode('utf-8'), callback=kafka_delivery_report)
            print(f"Produced msg ({msg_counter}) : {data} || delivered to {config.topic}")
            logging.info(f"Produced msg ({msg_counter}) : {data} || delivered to {config.topic}")
            msg_counter = msg_counter + 1
            producer.poll(0.5)  # Poll for delivery reports
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


# http://localhost:5000/produce

if __name__ == '__main__':
    handler = RotatingFileHandler('produce.log', maxBytes=1024 * 1024, backupCount=3)
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)

    app.run(debug=True, port=5000)

