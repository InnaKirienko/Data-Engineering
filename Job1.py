import os
import requests
import json
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

AUTH_TOKEN = os.environ.get('AUTH_TOKEN')
BASE_DIR = os.environ.get("BASE_DIR")

def clear_directory(directory):
    # Очистка вмісту директорії
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))


def fetch_sales_data(date, page):
    # Витягнення даних з API
    url = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
    params = {'date': date, 'page': page}
    headers = {'Authorization': AUTH_TOKEN}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 404 :
        return "requested page doesn't exist"
    else:
        return response.json()


def save_sales_data_to_file(directory, date, page, data):
    # Зберігання даних у файл
    filename = f"sales_{date}_{page}.json"
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f)


@app.route('/', methods=['POST'])
def handle_post_request():
    # Отримання JSON-об'єкту з запиту
    data = request.json

    # Перевірка, чи існує об'єкт та його властивості
    if data and 'date' in data and 'raw_dir' in data:
        date = data['date']
        raw_dir = data['raw_dir']

        # Створення шляху для зберігання
        directory = os.path.join(raw_dir, 'sales', date)
        os.makedirs(directory, exist_ok=True)

        # Очищення вмісту директорії
        clear_directory(directory)

        page = 1
        while True:
            sales_data = fetch_sales_data(date, page)
            if sales_data == "requested page doesn't exist":
                break
            else:
                save_sales_data_to_file(directory, date, page, sales_data)
                page += 1

        return jsonify({'message': 'Sales data fetched and saved successfully'}), 200
    else:
        return jsonify({'error': 'Invalid JSON format'}), 400


if __name__ == '__main__':
    app.run(port=8081)


