import json
import avro.datafile
import avro.schema

# Завантажити JSON-дані
with open('data.json', 'r') as f:
    json_data = json.load(f)


# Завантажити схему Avro
with open('schema.avsc', 'r') as f:
    avro_schema = avro.schema.parse(f)

# Перетворити JSON в Avro
avro_data = []
for json_record in json_data:
    avro_record = avro.datafile.write_record(avro_schema, json_record, None)
    avro_data.append(avro_record)

# Записати Avro-дані у файл
with open('data.avro', 'wb') as f:
    avro.datafile.write_data(avro_schema, avro_data, f)