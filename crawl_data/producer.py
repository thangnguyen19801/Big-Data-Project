from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor
from crawl import crawl_product_id, crawl_product, adjust_product

# Kafka configuration
bootstrap_servers = ['localhost:9092']
topic = 'tiki'

info = {
    "phone_and_tablet": "https://tiki.vn/api/personalish/v1/blocks/listings?page={}&limit=300&category=1789&urlKey=dien-thoai-may-tinh-bang",
    "electric_accessory": "https://tiki.vn/api/personalish/v1/blocks/listings?page={}&limit=300&aggregations=2&category=1815&urlKey=thiet-bi-kts-phu-kien-so",
    "camera": "https://tiki.vn/api/personalish/v1/blocks/listings?page={}&limit=300&category=1801&urlKey=may-anh",
    "man_fashion": "https://tiki.vn/api/personalish/v1/blocks/listings?page={}&limit=300&category=915&urlKey=thoi-trang-nam",
    "jewelry": "https://tiki.vn/api/personalish/v1/blocks/listings?limit=300&category=8371&urlKey=dong-ho-va-trang-suc"
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
    keysList = list(info.keys())
    i = 0
    while(1):
        k = keysList[i]
        for j in range(10):
            product_list = crawl_product_id(url=info[k].format(j))
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
        i = i + 1
        if i == len(keysList):
            i = 0
    