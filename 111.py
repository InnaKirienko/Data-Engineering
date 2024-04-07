import os
import requests


AUTH_TOKEN = os.environ['AUTH_TOKEN']


def main():

    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': '2022-08-09', 'page': 2},
        headers={'Authorization': AUTH_TOKEN},
    )
    print("Response status code:", response.status_code)
    print("Response JSON", response.json())


if __name__ == '__main__':
    main()