import requests

""" Attempts to make a git request using specified url as a parameter, 
    if successful return found data in json format, if failed print error and reason"""
def get_data_as_json(request_url):
    request_obj = requests.get(request_url)
    try:
        request_obj.raise_for_status()
        return request_obj.json()
    except requests.exceptions.RequestException as error:
        print(f'You received Error: {error}')
    finally:
        print(f'You tried to do a GET request. Response Code: {request_obj.status_code}. Reason: {request_obj.reason}')

def connect_to_database():
    db_connection = None






def main():
    get_data_as_json('https://data.nasa.gov/resource/gh4g-9sfh.json')


if __name__ == '__main__':
    main()
