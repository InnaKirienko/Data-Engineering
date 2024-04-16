# sales_api.py

import requests
from settings import AUTH_TOKEN

def get_sales_data(date: str, page: int) -> dict:
    # Getting data from the API and uploading it in json format.
    url = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
    params = {'date': date, 'page': page}
    headers = {'Authorization': AUTH_TOKEN}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 404:
        return {"error": "requested page doesn't exist"}
    else:
        return response.json()
