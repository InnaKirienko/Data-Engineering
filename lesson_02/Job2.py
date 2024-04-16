import os
import json
import fastavro
from flask import Flask, request, jsonify

app2 = Flask(__name__)

# Cleaning the contents of the directory
def clear_directory(directory : str ) -> None:
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))


# Convert files from JSON to Avro format
def convert_json_to_avro(dir1 : str, dir2 : str ) -> None:
    schema_filepath = 'sales_schema.avsc'

    with open(schema_filepath, 'r') as schema_file:
        schema = json.load(schema_file)

    for filename in os.listdir(dir1):
        if filename.endswith('.json'):
            json_filepath = os.path.join(dir1, filename)
            avro_filename = filename[:-5] + ".avro"
            avro_filepath = os.path.join(dir2, avro_filename)
            with open(json_filepath, 'r') as json_file:
                data = json.load(json_file)
                with open(avro_filepath, 'wb') as avro_file:
                    fastavro.writer(avro_file, schema=schema, records=data)


@app2.route('/', methods=['POST'])
def handle_post_request():
    # Getting a JSON object from a request
    data = request.json

    # Checking whether the object and its properties exist
    if data and 'raw_dir' in data and 'stg_dir' in data:
        raw_dir = data['raw_dir']
        stg_dir = data['stg_dir']

        # Creating a path to store stg data (if not present)
        os.makedirs(stg_dir, exist_ok=True)
        clear_directory(stg_dir)

        # File transfer and conversion of JSON to Avro
        convert_json_to_avro(raw_dir,stg_dir)

        return jsonify({'message': 'JSON files converted to Avro and moved to stg directory successfully'}), 201
    else:
        return jsonify({'error': 'Invalid input data'}), 400

if __name__ == '__main__':
    app2.run(port=8082)


