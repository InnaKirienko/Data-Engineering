import os
import requests
from flask import Flask, request, jsonify

# Завантаження секретного токена з config.py
AUTH_TOKEN = os.environ['AUTH_TOKEN']

# Створення Flask-додатка
app = Flask(__name__)

# Функція для обробки POST-запитів
@app.route('/sales', methods=['POST'])
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
        with open(os.path.join(raw_dir, filename), 'w') as f:
            json.dump(sales_data, f)

        # Перевірка на останню сторінку
        if sales_data['meta']['next_page'] is None:
            break

        page += 1

    return jsonify({"success": True}), 200

# Запуск Flask-додатка
if __name__ == '__main__':
    app.run(port=8081)
