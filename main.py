from database_functions import *


def main():
    """ This function call the necessary functions from the database_functions module
        that are required for the program to fully run, each object(response, json, connection,
        cursor) is assigned to a variable, so it can easily use as parameters
        for future functions."""
    response_obj = issue_get_request('https://data.nasa.gov/resource/gh4g-9sfh.json')
    json_obj = convert_content_to_json(response_obj)
    db_connection = connect_to_database()
    db_cursor_obj = create_cursor_obj(db_connection)
    create_all_region_tables(db_cursor_obj)
    add_meteorites_to_tables(db_cursor_obj, json_obj)
    close_database(db_connection, db_cursor_obj)


if __name__ == '__main__':
    main()
