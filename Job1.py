import os
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

AUTH_TOKEN = os.environ.get('AUTH_TOKEN')
# BASE_DIR = os.environ.get("BASE_DIR")

if not AUTH_TOKEN:
    print("Помилка: Змінна середовища AUTH_TOKEN не встановлена!")
    exit(1)


def clear_directory(directory: str) -> None:
    #Очищення вмісту директорії.
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))


def get_sales_data(date: str, page: int) -> dict:
    #Отримання даних з API та завантаження у форматі json.
    url = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
    params = {'date': date, 'page': page}
    headers = {'Authorization': AUTH_TOKEN}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 404:
        return {"error": "requested page doesn't exist"}
    else:
        return response.json()


def save_sales_data_to_file(directory: str, date: str, page: int, data: dict) -> None:
    #Збереження даних у файл
    filename = f"sales_{date}_{page}.json"
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f)


@app.route('/', methods=['POST'])
def handle_post_request():
    # Обробка POST-запиту
    # Отримання JSON-об'єкту з запиту
    data = request.json

    # Перевірка, чи існує об'єкт та його властивості
    if data and 'date' in data and 'raw_dir' in data:
        date = data['date']
        raw_dir = data['raw_dir']

        # Створення шляху для зберігання, якщо відсутній
        os.makedirs(raw_dir, exist_ok=True)

        # Очищення вмісту директорії
        clear_directory(raw_dir)

        # Отримання даних з API та завантаження їх у файл, у вказану запитом директорію
        page = 1
        while True:
            sales_data = get_sales_data(date, page)
            if "error" in sales_data:
                break
            else:
                save_sales_data_to_file(raw_dir, date, page, sales_data)
                page += 1

        return jsonify({'message': 'Sales data fetched and saved successfully'}), 201
    else:
        return jsonify({'error': 'Invalid input data'}), 400


if __name__ == '__main__':
    app.run(port=8081)
