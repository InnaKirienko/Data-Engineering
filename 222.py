from flask import Flask, request

app = Flask(__name__)

@app.route("/api", methods=["POST"])
def api():
    data = request.get_json()
    print(f"Дата: {data['date']}")
    print(f"Папка з даними: {data['raw_dir']}")
    return "OK"

if __name__ == "__main__":
    app.run(port=8081)