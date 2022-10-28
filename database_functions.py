"""
This module handles functions involving gathering of the meteor database data, and the creation of the database.
"""
from utility_functions import *
import sqlite3
import requests


def _create_bounding_boxes():
    """ This function creates a dictionary of geolocation bounding boxes
    and returns it. """

    # The bounding boxes contain a tuple with four values
    # (left - long min,bottom - lat min,right - long max,top - lat max)
    # which represent the borders of the box.
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
    # If the dictionary record has no key return false, otherwise return true.
    if dict_record.get(key, None) is None:
        return False
    else:
        return True


def issue_get_request(request_url):
    """ This function attempts to issue a GET request to the URL passed as its parameter.
        A response object is returned with a status code if no exception occurs,
        when an exception occurs it prints an error with status code. """

    try:
        # try to create make a get request and assign it a
        # response object with the specified url
        response_obj = requests.get(request_url)
        # pass any response code that isn't 200(OK) as an exception
        response_obj.raise_for_status()
    except requests.exceptions.RequestException as request_error:
        # If a request exception occurs, print the error in a formatted message.
        print(f'An error has occurred while issuing a GET request.\n'
              f'Response: {request_error}\n'
              f'Status Code: {request_error.response} ')
        # return the request error
        return request_error
    # If no exception occurs, print a GET request successful message with a status code.
    print(f'GET request successful. Response: {response_obj.status_code}. Status Code: {response_obj.status_code}\n')
    # Return the response object.
    return response_obj


def convert_content_to_json(response_obj):
    """ This function attempts to convert the contents of a specified response object to a json
        the function will throw an error if a JSON decoder error occurs and will return None,
        if successful it will return the json object. """
    # Create an empty json object.
    json_data_obj = None
    try:
        # Try to assign the json object the response object passed as a parameter
        # using the JSON decoder.
        json_data_obj = response_obj.json()
        # Print a message that the json object was converted.
        print(f'Response object content converted to JSON object.\n')
    except requests.exceptions.JSONDecodeError as json_decode_error:
        # If any JSON decoder exceptions occur, print an error in a formatted message.
        print(f'An error has occurred while trying to convert the response content to a JSON object.\n'
              f'{json_decode_error}')
    finally:
        # return the JSON object even if an exception occurs
        return json_data_obj


def connect_to_database():
    """ This function attempts to set up a connection with the sqlite database.
        If any sqlite exceptions occur it will print an error
        and will return the connection as none. If successful it will return the connection."""
    # Create an empty connection object.
    db_connection = None
    try:
        # Try to assign a name to the database and connect it.
        db_name = 'meteorite_db_all.db'
        db_connection = sqlite3.connect(db_name)
    except sqlite3.Error as db_error:
        # If a sqlite error occurs print it a formatted message.
        print(f'A database error has occurred: {db_error}')
    finally:
        # Return the connection even if unsuccessful.
        return db_connection


def create_cursor_obj(db_connection):
    """ This function attempts to create a cursor object for the database passing the connection
        as a parameter. If any sqlite exceptions occur it will print an error
        and will return the cursor as none. If successful it will return the cursor. """
    # Create an empty cursor object.
    db_cursor_obj = None
    try:
        # Try to assign the cursor object to the connection passed as a parameter.
        db_cursor_obj = db_connection.cursor()
    except sqlite3.Error as db_error:
        # If a sqlite error occurs print it a formatted message.
        print(f'A database error has occurred: {db_error}')
    finally:
        # Return the cursor object even if unsuccessful.
        return db_cursor_obj


def create_all_region_tables(db_cursor_obj):
    """ This function attempts to create all seven tables for each region
        with the specified cursor object as a parameter, this function creates a table
        for each region if it doesn't exist. It will delete any entries from each table if already
        filled. If any sqlite exceptions occur, it will print an error. """
    try:
        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Africa_MiddleEast_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM Africa_MiddleEast_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Europe_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM Europe_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Upper_Asia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM Upper_Asia_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Lower_Asia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM Lower_Asia_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS Australia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM Australia_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS North_America_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM North_America_Meteorites''')

        # Execute Sqlite3 CREATE TABLE function on the cursor object only if it doesn't exist.
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS South_America_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        # Execute Delete from table on the cursor object.
        # Delete any data from the table if it already existed.
        db_cursor_obj.execute('''DELETE FROM South_America_Meteorites''')
    except sqlite3.Error as db_error:
        # If any sqlite exceptions occur, print the error in a formatted message.
        print(f'A database error has occurred: {db_error}')


def _add_to_region_africa_middle(db_cursor_obj, record):
    """ This function adds a record to the Africa_MiddleEast_Meteorites
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO Africa_MiddleEast_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_europe(db_cursor_obj, record):
    """ This function adds a record to the Europe_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO Europe_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_upper_asia(db_cursor_obj, record):
    """ This function adds a record to the Upper_Asia_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO Upper_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_lower_asia(db_cursor_obj, record):
    """ This function adds a record to the Lower_Asia_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO Lower_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_australia(db_cursor_obj, record):
    """ This function adds a record to the Australia_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO Australia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_north_america(db_cursor_obj, record):
    """ This function adds a record to the North_America_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO North_America_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def _add_to_region_south_america(db_cursor_obj, record):
    """ This function adds a record to the South_America_Meteorites table
        with the record specified through a parameter. If any
        sqlite exceptions occur, an error will print."""
    try:
        db_cursor_obj.execute('''INSERT INTO South_America_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')


def add_meteorites_to_tables(db_cursor_obj, json_data_obj):
    """ This functions adds a meteor from the JSON data object to the corresponding table depending on if
    its reclat and reclong values fall between a certain bounding box. The function loops through every meteor in the JSON object.
    The tuple values of each box are called in an if statement to check if the meteors
    latitude and longitude fall within the values of the bounding box. """
    # Call create_bounding_boxes function to create the bounding boxes
    bounding_boxes = _create_bounding_boxes()
    # Get the tuple values of each bounding box and assign it to a corresponding variable
    africa_middle_values = bounding_boxes.get('Africa_MiddleEast_Meteorites')
    europe_values = bounding_boxes.get('Europe_Meteorites')
    upper_asia_values = bounding_boxes.get('Upper_Asia_Meteorites')
    lower_asia_values = bounding_boxes.get('Lower_Asia_Meteorites')
    australia_values = bounding_boxes.get('Australia_Meteorites')
    north_america_values = bounding_boxes.get('North_America_Meteorites')
    south_america_values = bounding_boxes.get('South_America_Meteorites')
    try:
        # Loop through each meteor in the JSON object, if the JSON object is empty,
        # print an error message.
        for record in json_data_obj:
            # Check first if the specified meteor has a reclat and reclong value
            # If not it will skip the meteor entry.
            if _check_dict_has_key(record, 'reclat')\
                    and _check_dict_has_key(record, 'reclong'):
                # Assign the latitude and longitude values of the current meteor
                # to variables. It will first convert the values into floats using
                # the convert function the utility functions file. As the values
                # in the JSON file are strings.
                lat_value = convert_string_to_numerical(record.get('reclat'))
                long_value = convert_string_to_numerical(record.get('reclong'))
                # Check both the latitude values and longitude values of the meteor see if they fit
                # in the range of the bounding box. The corresponding indexes are called
                # from the values of the bounding box to properly check. If the meteor
                # falls into one of the bounding boxes, call the corresponding add to table function
                # on that meteor as a parameter.
                if africa_middle_values[0] <= long_value <= africa_middle_values[2] and \
                        africa_middle_values[1] <= lat_value <= africa_middle_values[3]:
                    _add_to_region_africa_middle(db_cursor_obj, record)
                if europe_values[0] <= long_value <= europe_values[2] and \
                        europe_values[1] <= lat_value <= europe_values[3]:
                    _add_to_region_europe(db_cursor_obj, record)
                if upper_asia_values[0] <= long_value <= upper_asia_values[2] and \
                        upper_asia_values[1] <= lat_value <= upper_asia_values[3]:
                    _add_to_region_upper_asia(db_cursor_obj, record)
                if lower_asia_values[0] <= long_value <= lower_asia_values[2] and \
                        lower_asia_values[1] <= lat_value <= lower_asia_values[3]:
                    _add_to_region_lower_asia(db_cursor_obj, record)
                if australia_values[0] <= long_value <= australia_values[2] and \
                        australia_values[1] <= lat_value <= australia_values[3]:
                    _add_to_region_australia(db_cursor_obj, record)
                if north_america_values[0] <= long_value <= north_america_values[2] and \
                        north_america_values[1] <= lat_value <= north_america_values[3]:
                    _add_to_region_north_america(db_cursor_obj, record)
                if south_america_values[0] <= long_value <= south_america_values[2] and \
                        south_america_values[1] <= lat_value <= south_america_values[3]:
                    _add_to_region_south_america(db_cursor_obj, record)
            # If the meteor doesn't fit in any bounding box, skip it.
            else:
                continue
    except TypeError:
        # A typeError occurs if the JSON file is empty. Which can result from not properly
        # doing a GET request on the data.
        print('A TypeError has occurred, your JSON file could be empty! Did you GET request work correctly?')


def close_database(db_connection, db_cursor_obj):
    """ This function will close the specified connection, it will attempt to commit the database
        and close database before closing. If any sqlite exceptions occur, it will print an error. If
        sucessful it closes the database and print a message if it hasn't already closed. """
    try:
        # Attempt to commit the database and close the cursor
        db_connection.commit()
        db_cursor_obj.close()
    except sqlite3.Error as db_error:
        # If any sqlite exceptions occur, print the error in a formatted message.
        print(f'A Database Error has occurred: {db_error}')
    finally:
        # Close the connection if it still exists and print a message. This code executes
        # even if there was an exception.
        if db_connection:
            db_connection.close()
            print('Database connection closed.')
