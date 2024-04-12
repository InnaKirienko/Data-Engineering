import os
import json
import fastavro

raw_dir = "C:\\Users\\user\\Documents\\Data-Engeniring\\file_storage\\raw\\sales\\2022-08-09"
stg_dir = "C:\\Users\\user\\Documents\\Data-Engeniring\\file_storage\\stg\\sales\\2022-08-09"

def clear_directory(directory):
    # Очистка вмісту директорії
    file_list = os.listdir(directory)
    for file in file_list:
        os.remove(os.path.join(directory, file))

def convert_json_to_avro2(dir1,dir2):
    # Конвертування файлів з JSON у Avro формат
    for filename in os.listdir(dir1):
        if filename.endswith('.json'):
            json_filepath = os.path.join(dir1, filename)
            avro_filename = filename[:-5] + ".avro"  # Зміна розширення
            avro_filepath = os.path.join(dir2, avro_filename)
            with open(json_filepath, 'r') as json_file:
                data = json.load(json_file)
                schema = data[0].get('schema')
                records = data[0].get('records')
                with open(avro_filepath, 'wb') as avro_file:
                    fastavro.writer(avro_file, schema=schema, records=records)
'''
def convert_json_to_avro(dir1, dir2):
    # Конвертування файлів з JSON у Avro формат
    for filename in os.listdir(dir1):
        if filename.endswith('.json'):
            json_filepath = os.path.join(dir1, filename)
            avro_filename = filename[:-5] + ".avro"  # Зміна розширення
            avro_filepath = os.path.join(dir2, avro_filename)
            with open(json_filepath, 'r') as json_file:
                data = json.load(json_file)
                # Зчитуємо схему та записи з JSON
                schema = data.get('schema')
                records = data.get('records')
                if schema and records:
                    # Відкриваємо файл для запису Avro
                    with open(avro_filepath, 'wb') as avro_file:
                        # Записуємо дані у форматі Avro
                        fastavro.writer(avro_file, schema=schema, records=records)



import avro.schema
import avro.io

def convert_json_to_avro(raw_dir, avro_dir):
    for filename in os.listdir(raw_dir):
        if filename.endswith('.json'):
            json_filepath = os.path.join(raw_dir, filename)
            avro_filename = filename[:-5] + ".avro"
            avro_filepath = os.path.join(avro_dir, avro_filename)
            try:
                with open(json_filepath, 'r') as json_file:
                    data = json.load(json_file)
                    # Побудова схеми Avro з вмісту JSON-файлу
                    schema = avro.schema.make_avsc_object(data[0])
                    print("Schema for", filename, ":", schema)
            except Exception as e:
                print("Error processing file", filename, ":", str(e))

os.makedirs(stg_dir, exist_ok=True)
clear_directory(stg_dir)
convert_json_to_avro2(raw_dir,stg_dir)



json_filepath = os.path.join(raw_dir, 'sales_2022-08-09_1.json')
with open(json_filepath, 'r') as json_file:
    data = json.load(json_file)
#schema = data[0].get('schema')
records = data.get('records')
#print(schema)
print(records)
'''

from fastavro import json_reader

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

json_filepath = os.path.join(raw_dir, 'sales_2022-08-09_1.json')

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

        records = data
        print(schema)
        print(records)
'''  
    with open(json_filepath, 'r') as fo:
    avro_reader = json_reader(fo, schema)
    for record in avro_reader:
        print(record)
print(avro_reader)
'''