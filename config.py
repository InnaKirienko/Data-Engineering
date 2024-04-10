import os
import json
import fastavro
from flask import Flask, request, jsonify

app = Flask(__name__)

def clear_directory(directory):
    # Очистка вмісту директорії
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))

def convert_json_to_avro(directory):
    # Конвертування файлів з JSON у Avro формат
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            json_filepath = os.path.join(directory, filename)
            avro_filename = filename[:-5] + ".avro"  # Зміна розширення
            avro_filepath = os.path.join(directory, avro_filename)
            with open(json_filepath, 'r') as json_file:
                data = json.load(json_file)
                schema = data.get('schema')
                records = data.get('records')
                with open(avro_filepath, 'wb') as avro_file:
                    fastavro.writer(avro_file, schema=schema, records=records)
            #os.remove(json_filepath)  # Видалення оригінального JSON файлу

@app.route('/', methods=['POST'])
def handle_post_request():
    # Отримання JSON-об'єкту з запиту
    data = request.json

    # Перевірка, чи існує об'єкт та його властивості
    if data and 'raw_dir' in data and 'stg_dir' in data:
        raw_dir = data['raw_dir']
        stg_dir = data['stg_dir']

        # Перенесення файлів та конвертація JSON в Avro
        convert_json_to_avro(raw_dir)

        # Копіювання файлів формату Avro в stg директорію
        for filename in os.listdir(raw_dir):
            src_path = os.path.join(raw_dir, filename)
            dst_path = os.path.join(stg_dir, filename)
            os.rename(src_path, dst_path)

        return jsonify({'message': 'JSON files converted to Avro and moved to stg directory successfully'}), 200
    else:
        return jsonify({'error': 'Invalid JSON format'}), 400

if __name__ == '__main__':
    app.run(port=8082)
