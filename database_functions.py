"""
This module handles functions involving gathering of the meteor database, and it's creation
"""
from utility_functions import *
import sqlite3
import requests


def _create_bounding_boxes():
    """ This function creates a dictionary of bounding boxes and returns it. """
    bound_box_dict = {
        'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
        'Europe_Meteorites': (-24.1, 36, 32, 71.1),
        'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
        'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
        'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
        'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
        'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
    }
    return bound_box_dict


def _check_dict_has_key(dict_record, key):
    """ This function checks to see if a specified dictionary record has a key specified from
        the parameters. If the key is none it will return false, otherwise it will return true.
    """
    if dict_record.get(key, None) is None:
        return False
    else:
        return True


def _check_meteor_in_bounding_box(dict_record, key_lat, key_long):
    """ This function checks if a specified dictionary record has a latitude and longitude
        within the range of the created bounding boxes called from the bounding box function,
        it will loop through each bounding box, if both latitude and longitude are within range
        it will return the specified bounding box,
        otherwise it will continue to the next bounding box. """
    bound_box_dict = _create_bounding_boxes()
    for box_key in bound_box_dict:
        box_values = bound_box_dict.get(box_key)
        lat_value = convert_string_to_numerical(dict_record.get(key_lat))
        long_value = convert_string_to_numerical(dict_record.get(key_long))
        if box_values[3] <= lat_value > box_values[1]:
            if box_values[2] <= long_value > box_values[0]:
                return box_key
            else:
                continue
        else:
            continue


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
    print(f'GET request successful. Response: {response_obj.status_code}. Status Code: {response_obj.status_code}\n')
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
    """ This function attempts to set up a connection with the sqlite database.
        If any sqlite exceptions occur it will print an error
        and will return the connection as none. If successful it will return the connection."""
    db_connection = None
    try:
        db_name = 'meteorite_db_all.db'
        db_connection = sqlite3.connect(db_name)
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')
    finally:
        return db_connection


def create_cursor_obj(db_connection):
    """ This function attempts to create a cursor object for the database passing the connection
        as a parameter. If any sqlite exceptions occur it will print an error
        and will return the cursor as none. If successful it will return the cursor. """
    db_cursor_obj = None
    try:
        db_cursor_obj = db_connection.cursor()
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')
    finally:
        return db_cursor_obj


def create_all_region_tables(db_cursor_obj):
    """ This function attempts to create all seven tables for each region
        with the specified cursor object as a parameter, this function creates a table
        for each region if it doesn't exist. It will delete any entries from each table if already
        filled. If any sqlite exceptions occur, it will print an error. """
    try:
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Africa_MiddleEast_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM Africa_MiddleEast_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Europe_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM Europe_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Upper_Asia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM Upper_Asia_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Lower_Asia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM Lower_Asia_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Australia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM Australia_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS North_America_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM North_America_Meteorites''')

        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS South_America_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor_obj.execute('''DELETE FROM South_America_Meteorites''')
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def add_meteorites_to_tables(db_cursor_obj,
                             json_data_obj):
    """ This function loops through every record in the json file, it checks every record to see if
        it has both the reclat and reclong key if it doesn't skip to the next entry, if it does call the function
        that checks what bounding box the record fits in and assign it to a string variable, the entry
        will then be added to the table depending on the name of the string variable. If any sqlite exceptions
        occur while adding records, it will print an error."""
    for record in json_data_obj:
        if _check_dict_has_key(record, 'reclat') & _check_dict_has_key(record, 'reclong'):
            table_name = _check_meteor_in_bounding_box(record, 'reclat', 'reclong')
            try:
                if table_name == 'Africa_MiddleEast_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO Africa_MiddleEast_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'Europe_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO Europe_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'Upper_Asia_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO Upper_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'Lower_Asia_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO Lower_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'Australia_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO Australia_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'North_America_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO North_America_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))

                elif table_name == 'South_America_Meteorites':
                    db_cursor_obj.execute('''INSERT INTO South_America_Meteorites VALUES(?, ?, ?, ?)''',
                                          (record.get('name', None),
                                           record.get('mass', None),
                                           record.get('reclat', None),
                                           record.get('reclong', None)))
            except sqlite3.Error as db_error:
                print(f'A database error has occurred: {db_error}')
        else:
            continue


def close_database(db_connection, db_cursor_obj):
    """ This function will close the specified connection, it will attempt to commit the database
        and close database before closing. If any sqlite exceptions occur, it will print an error. If
        sucessful it closes the database and print a message if it hasn't already closed. """
    try:
        db_connection.commit()
        db_cursor_obj.close()
    except sqlite3.Error as db_error:
        print(f'A Database Error has occurred: {db_error}')
    finally:
        if db_connection:
            db_connection.close()
            print('Database connection closed.')
