import os
import requests
import json
from flask import Flask, request, jsonify
from settings import AUTH_TOKEN
from sales_api import get_sales_data

app = Flask(__name__)


if not AUTH_TOKEN:
    print("Error: ENVIRONMENT VARIABLE AUTH_TOKEN NOT SET!")
    exit(1)


def clear_directory(directory: str) -> None:
    #Cleaning the contents of the directory
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))


def save_sales_data_to_file(directory: str, date: str, page: int, data: dict) -> None:
    #Saving data to a file
    filename = f"sales_{date}_{page}.json"
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f)


@app.route('/', methods=['POST'])
def handle_post_request():
    # POST request processing
    # Getting a JSON object from a request
    data = request.json

    # Checking whether the object and its properties exist
    if data and 'date' in data and 'raw_dir' in data:
        date = data['date']
        raw_dir = data['raw_dir']

        # Creating a storage path if none exists
        os.makedirs(raw_dir, exist_ok=True)

        # Cleaning the contents of the directory
        clear_directory(raw_dir)

        # Receiving data from the API and uploading them to a file in the directory specified by the request
        page = 1

        while page > 0:  # Condition to stop when "error" is encountered
            sales_data = get_sales_data(date, page)
            if "error" in sales_data:
                page = 0  # Set page to 0 to exit the loop
            else:
                save_sales_data_to_file(raw_dir, date, page, sales_data)
                page += 1


        return jsonify({'message': 'Sales data fetched and saved successfully'}), 201
    else:
        return jsonify({'error': 'Invalid input data'}), 400


if __name__ == '__main__':
    app.run(port=8081)
