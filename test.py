import os
import requests
import json
from flask import Flask, request, jsonify


AUTH_TOKEN = os.environ['AUTH_TOKEN']
BASE_DIR = os.environ.get("BASE_DIR")

# = {"date": "2022-08-09",
#            "raw_dir": RAW_DIR}

app = Flask(__name__)
@app.route('/sales', methods=['POST'])

# обробка POST-запитів
def sales():
    # Отримання JSON-об'єкта з запиту
    data = request.get_json()

    # Перевірка даних
    if not data or 'date' not in data or 'raw_dir' not in data:
        return jsonify({"error": "Invalid request data"}), 400

    # Очищення директорії raw_dir
    raw_dir = data['raw_dir']
    for filename in os.listdir(raw_dir):
        os.remove(os.path.join(raw_dir, filename))

    # Отримання даних продажів з API
    page = 1
    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': data['date'], 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )

        if response.status_code != 200:
            return jsonify({"error": "API error"}), 500

        # Збереження даних
        sales_data = response.json()
        filename = f"sales_{data['date']}"
        if page > 1:
            filename += f"_{page}"
        filename += ".json"
        with open(os.path.join(BASE_DIR, '', filename), 'w') as f:
            json.dump(sales_data, f)

        # Перевірка на останню сторінку
        if sales_data['meta']['next_page'] is None:
            break

        page += 1

    return jsonify({"success": True}), 200

# Запуск Flask-додатка
if __name__ == '__main__':
    app.run(port=8081)