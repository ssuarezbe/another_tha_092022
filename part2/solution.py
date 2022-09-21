#!/usr/bin/python3
import os
import sqlite3
from part2.utils import (
    read_patient_csv, read_procedure_csv,
    prepate_procedures_text_search_db,
    _search_procedure_orders,
    create_db_tables,
    insert_procedure_rows,
    insert_patient_rows,
    _patients_with_procedure,
    _map_orders_to_patients
)


def search_procedure_orders(search_pattern, procedure_orders_data):
    db_file_path = os.path.join('/tmp/part2_qventus.db')
    with sqlite3.connect(db_file_path) as connection:
        cursor = connection.cursor()
        prepate_procedures_text_search_db(procedure_orders_data, cursor, connection)
        r = _search_procedure_orders(search_pattern=search_pattern, cursor=cursor)
        print("\n\nPART 2 - QUESTION 1")
        print("=== search_procedure_orders ===")
        print(f"Search pattern = '{search_pattern}'")
        print("--- result set -----")
        print(r)
    return r


def map_procedure_orders_to_patients(patient_data, procedure_orders_data):
    db_file_path = os.path.join('/tmp/part2_qventus.db')
    with sqlite3.connect(db_file_path) as connection:
        cursor = connection.cursor()
        print("\n\nPART 2 - QUESTION 2")
        print("=== map_orders_to_patients ===")
        create_db_tables(cursor)
        insert_procedure_rows(cursor, connection, procedure_orders_data)
        insert_patient_rows(cursor, connection, patient_data)
        r = _map_orders_to_patients(cursor)
        print("--- result set -----")
        print(r)
    return r


def patients_with_procedure(patient_data, procedure_orders_data, search_pattern):
    db_file_path = os.path.join('/tmp/part2_qventus.db')
    with sqlite3.connect(db_file_path) as connection:
        cursor = connection.cursor()
        print("\n\nPART 2 - QUESTION 3")
        print("=== patients_with_procedure ===")
        print(f"Search pattern = '{search_pattern}'")
        create_db_tables(cursor)
        insert_procedure_rows(cursor, connection, procedure_orders_data)
        insert_patient_rows(cursor, connection, patient_data)
        r = _patients_with_procedure(search_pattern=search_pattern, cursor=cursor)
        print("--- result set -----")
        print(r)
    return r

def main():
    procedure_rows = read_procedure_csv()
    patients_rows = read_patient_csv()
    search_pattern = "hospitalist*"
    r = search_procedure_orders(search_pattern, procedure_rows)
    r = map_procedure_orders_to_patients(patients_rows, procedure_rows)
    r = patients_with_procedure(patients_rows, procedure_rows, search_pattern)



if __name__ == '__main__':
    main()
