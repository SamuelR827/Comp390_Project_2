"""
This module handles functions involving gathering of the meteor database and it's creation
"""
from utility_functions import *
import sqlite3
import requests


def issue_get_request(request_url: str):
    """ This function attempts to issue a GET request to the URL passed as its parameter.
        A response object is returned with a status code if no exception occurs,
        when an exception occurs it prints an error with status code. """
    try:
        response_obj = requests.get(request_url)
        response_obj.raise_for_status()
    except requests.exceptions.RequestException as request_error:
        print(f'An error has occurred while issuing a GET request.\n'
              f'Response: {request_error}\n'
              f'Status Code: {request_error.response} ')
        return request_error
    print(f'GET request successful. Response: {response_obj.status_code}. Status Code: {response_obj.status_code}')
    return response_obj


def convert_content_to_json(response_obj: requests.Response):
    """ This function attempts to convert the contents of a specified response object to a json
        the function will throw an error if a JSON decoder error occurs and will return None,
        if successful it will return the json object. """
    json_data_obj = None
    try:
        json_data_obj = response_obj.json()
        print(f'Response object content converted to JSON object.\n')
    except requests.exceptions.JSONDecodeError as json_decode_error:
        print(f'An error has occurred while trying to convert the response content to a JSON object.\n'
              f'{json_decode_error}')
    finally:
        return json_data_obj


def connect_to_database():
    """ This function attempts to set up a connection with the sqlite database
        and create a cursor object, if any sqlite exceptions occur it will print an error"""
    db_connection = None
    db_cursor_obj = None
    try:
        db_name = 'meteorite_db_all.db'
        db_connection = sqlite3.connect(db_name)
        db_cursor_obj = db_connection.cursor()
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')
    finally:
        return db_cursor_obj


def create_table(cursor_obj: connect_to_database(), table_name: str):
    """ This function attempts to create a table with the specified cursor object
        and table name as parameters, if any sqlite exceptions occur it will print an error """
    try:
        cursor_obj.execute('''CREATE TABLE IF NOT EXISTS [table_name](
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def create_all_region_tables(cursor_obj: connect_to_database()):
    """ This function attempts to create all seven tables for each region
        with the specified cursor object as a parameter, this function calls
        the create_table seven times with the specified name """
    create_table(cursor_obj, table_name='Africa_MiddleEast_Meteorites')
    create_table(cursor_obj, table_name='Europe_Meteorites')
    create_table(cursor_obj, table_name='Upper_Asia_Meteorites')
    create_table(cursor_obj, table_name='Lower_Asia_Meteorites')
    create_table(cursor_obj, table_name='Australia_Meteorites')
    create_table(cursor_obj, table_name='North_America_Meteorites')
    create_table(cursor_obj, table_name='South_America_Meteorites')



