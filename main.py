from database_functions import *

def main():
    response_obj = issue_get_request('https://data.nasa.gov/resource/gh4g-9sfh.json')
    json_obj = convert_content_to_json(response_obj)
    db_cursor_object = connect_to_database()
    create_all_region_tables(db_cursor_object)


if __name__ == '__main__':
    main()
