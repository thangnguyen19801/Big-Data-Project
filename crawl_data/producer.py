from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor
from crawl import crawl_product_id, crawl_product, adjust_product

# Kafka configuration
bootstrap_servers = ['localhost:9092']
topic = 'tiki'

info = {
    "phone_and_tablet": "https://tiki.vn/api/personalish/v1/blocks/listings?limit=5&category=1789&urlKey=dien-thoai-may-tinh-bang"
}

def serializer(message):
    return json.dumps(message).encode('utf-8')

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=serializer,
    key_serializer=serializer,
)

# Run the scraper at regular intervals
def send_message():
    for k in info:
        product_list = crawl_product_id(url=info[k])
        print("No. Product ID: ", len(product_list))

        product_list = crawl_product(product_list)
        product_json_list = [adjust_product(p) for p in product_list]
        debug = True  # Set to True to enable debug output
        with ThreadPoolExecutor(max_workers=16) as executor:
            for product in product_json_list:
                if debug:
                    print(f"Sending record to kafka Stream: {product}")
                    
                    producer.send('tiki', product, k)
                    producer.flush()    
        product_list = crawl_product_id(url=info[k])
        print("No. Product ID: ", len(product_list))

if __name__ == "__main__":
    send_message()
    