from flask import Flask, jsonify
import pymongo

app = Flask(__name__)

# MongoDB configuration
client = pymongo.MongoClient("mongodb://localhost:27017/")  # mongodb is the service name from docker-compose.yml
db = client["E-Commerce"]  # Replace "mydatabase" with your desired database name

# Example: Insert a document into a collection
collection = db["Tiki"]

# API endpoint to get all documents from the collection
@app.route('/data', methods=['GET'])
def get_data():
    cursor = list(collection.find().limit(10))   # Retrieve all documents from the collection
    data = []
    for document in cursor:
    # Convert ObjectId to string
        value = document.pop('_id')
        data.append(document) 
    return jsonify(data), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)