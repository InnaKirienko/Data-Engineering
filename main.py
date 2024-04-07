from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_post_request():
    # Отримання JSON-об'єкту з запиту
    data = request.json

    # Перевірка, чи існує об'єкт та його властивості
    if data and 'date' in data and 'raw_dir' in data:
        date = data['date']
        raw_dir = data['raw_dir']

        # Тут можна виконати необхідні дії з отриманими даними

        return jsonify({'message': 'Request received successfully', 'date': date, 'raw_dir': raw_dir}), 200
    else:
        return jsonify({'error': 'Invalid JSON format'}), 400


if __name__ == '__main__':
    app.run(port=8081)
