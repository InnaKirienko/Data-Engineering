import os
import json
import fastavro
from flask import Flask, request, jsonify

app2 = Flask(__name__)

# Очистка вмісту директорії
def clear_directory(directory : str ) -> None:
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))


# Конвертування файлів з JSON у Avro формат
def convert_json_to_avro(dir1 : str, dir2 : str ) -> None:
    # Конвертування файлів з JSON у Avro формат
    for filename in os.listdir(dir1):
        if filename.endswith('.json'):
            json_filepath = os.path.join(dir1, filename)
            avro_filename = filename[:-5] + ".avro"  # Зміна розширення
            avro_filepath = os.path.join(dir2, avro_filename)
            with open(json_filepath, 'r') as json_file:
                data = json.load(json_file)
                schema = {
                    "type": "record",
                    "name": "Sales",
                    "fields": [
                        {"name": "client", "type": "string"},
                        {"name": "purchase_date", "type": "string"},
                        {"name": "product", "type": "string"},
                        {"name": "price", "type": "float"}
                    ]
                }

                with open(avro_filepath, 'wb') as avro_file:
                    fastavro.writer(avro_file, schema=schema, records=data)


@app2.route('/', methods=['POST'])
def handle_post_request():
    # Отримання JSON-об'єкту з запиту
    data = request.json

    # Перевірка, чи існує об'єкт та його властивості
    if data and 'raw_dir' in data and 'stg_dir' in data:
        raw_dir = data['raw_dir']
        stg_dir = data['stg_dir']

        # Створення шляху для зберігання stg даних (якщо відсутній)
        os.makedirs(stg_dir, exist_ok=True)
        clear_directory(stg_dir)

        # Перенесення файлів та конвертація JSON в Avro
        convert_json_to_avro(raw_dir,stg_dir)

        return jsonify({'message': 'JSON files converted to Avro and moved to stg directory successfully'}), 201
    else:
        return jsonify({'error': 'Invalid input data'}), 400

if __name__ == '__main__':
    app2.run(port=8082)


