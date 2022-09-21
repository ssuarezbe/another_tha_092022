#!/usr/bin/python3
import csv
import os
import sqlite3
import pathlib
from datetime import datetime

current_path = pathlib.Path(__file__).parent.resolve()

def read_patient_csv():
    """
    Read csv data and parsed the data so is valid with Sqlite table schema
    """
    file_path = os.path.join(current_path,'../data','qventus_interview_patient_data_dataset.csv')
    transformed_rows = []
    date_fmt ="%Y-%m-%d %H:%M:%S"
    rows_count = 0
    with open(file_path, 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            rows_count += 1;
            admit_time = datetime.strptime(row[2], date_fmt)
            discharge_time = datetime.strptime(row[3], date_fmt)
            parsed_row = (int(row[0]), int(row[1]), admit_time, discharge_time, row[4], row[5], row[6])
            transformed_rows.append(parsed_row)
    if rows_count != len(transformed_rows):
        raise ValueError("Rows count mismatch {}=!{} after reading file qventus_interview_patient_data_dataset.csv".format(rows_count, len(rows)))
    return transformed_rows


def read_procedure_csv():
    """
    Read csv data and parsed the data so is valid with Sqlite table schema
    """
    file_path = os.path.join(current_path,'../data','qvenus_interview_procedure_orders.csv')
    transformed_rows = []
    date_fmt ="%Y-%m-%d %H:%M:%S"
    rows_count = 0
    with open(file_path, 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            rows_count += 1;
            d = datetime.strptime(row[3], date_fmt)
            parsed_row = (int(row[0]), int(row[1]), int(row[2]), d, row[4], row[5], row[6])
            transformed_rows.append(parsed_row)
    if rows_count != len(transformed_rows):
        raise ValueError("Rows count mismatch {}=!{} after reading file qvenus_interview_procedure_orders.csv".format(rows_count, len(rows)))
    return transformed_rows



def create_db_tables(cursor):
    """
    Create the tables structure in SQLite
    """
    drop_table_procedure = "DROP TABLE IF EXISTS procedure_order;"
    drop_table_patient = "DROP TABLE IF EXISTS patient_data;"

    # Table Definition
    create_table_procedure = '''
    CREATE TABLE procedure_order (
        id INTEGER not null primary key,
        procedure_id INTEGER  not null,
        encounter_id INTEGER not null,
        order_time DATETIME default null,
        procedure_name varchar(128) default null,
        order_class varchar(128) default null,
        order_variety varchar(128) default null,
        unique(procedure_id)
    );
    '''

    # Table Definition
    create_table_patient = '''
    CREATE TABLE patient_data (
        id int not null primary key,
        encounter_id int DEFAULT NULL,
        admit_time DATETIME DEFAULT NULL,
        discharge_time DATETIME DEFAULT NULL,
        discharge_department varchar(128) DEFAULT NULL,
        final_drg varchar(128) DEFAULT NULL,
        admitting_ICD10 varchar(128) DEFAULT NULL,
        unique(encounter_id)
        );
    '''
    cursor.execute(drop_table_procedure);
    cursor.execute(create_table_procedure);
    cursor.execute(drop_table_patient);
    cursor.execute(create_table_patient);
    return True


def insert_procedure_rows(cursor, connection, data):
    t_columns = ('id', 'procedure_id', 'encounter_id', 'order_time', 'procedure_name', 'order_class', 'order_variety')
    insertQuery = f"""INSERT INTO procedure_order {t_columns} VALUES (?, ?, ?, ?, ?, ?, ?);"""
    print(f"insertQuery={insertQuery}")
    # insert the data into table
    cursor.executemany(insertQuery, data)
    connection.commit()
    print("Data Inserted Successfully procedure_order !")
    print("Total {} rows inserted".format(len(data)))
    return True


def insert_patient_rows(cursor, connection, data):
    t_columns = ('id', 'encounter_id', 'admit_time', 'discharge_time', 'discharge_department', 'final_drg', 'admitting_ICD10')
    insertQuery = f"""INSERT INTO patient_data {t_columns} VALUES (?, ?, ?, ?, ?, ?, ?);"""
    print(f"insertQuery={insertQuery}")
    # insert the data into table
    cursor.executemany(insertQuery, data)
    connection.commit()
    print("Data Inserted Successfully in patient_data !")
    print("Total {} rows inserted".format(len(data)))
    return True


def prepate_procedures_text_search_db(data, cursor, connection):
    """
    Create a sqlite table that supports full text search
    """
    drop_table_ddl = "DROP TABLE IF EXISTS procedure_text_search;"
    table_creation_ddl = "CREATE VIRTUAL TABLE procedure_text_search USING fts5('csv_id', 'procedure_id', 'encounter_id', 'order_time', 'procedure_name', 'order_class', 'order_variety');"
    cursor.execute(drop_table_ddl);
    cursor.execute(table_creation_ddl);
    t_columns = ('csv_id', 'procedure_id', 'encounter_id', 'order_time', 'procedure_name', 'order_class', 'order_variety')
    insertQuery = f"""INSERT INTO procedure_text_search {t_columns} VALUES (?, ?, ?, ?, ?, ?, ?);"""
    print(f"insertQuery={insertQuery}")
    # insert the data into table
    cursor.executemany(insertQuery, data)
    connection.commit()
    print("Data Inserted Successfully procedure_order !")
    print("Total {} rows inserted".format(len(data)))
    return len(data)



def _search_procedure_orders(search_pattern, cursor):
    """
    Create a function called search_procedure_orders that takes in a search pattern
    and the procedure_orders_data and returns relevant procedure orders as an array.


    Use https://www.sqlite.org/fts5.html as text search engine

    Read this example to understand more about sqlite text search engine:
      https://www.sqlitetutorial.net/sqlite-full-text-search/
    """
    sqlite3_query_tmplt = f"SELECT * FROM procedure_text_search WHERE procedure_name MATCH '{search_pattern}';"
    print(f"SQLITE query: {sqlite3_query_tmplt}")
    total_result = list(cursor.execute(sqlite3_query_tmplt))
    # clean results
    return total_result


def _map_orders_to_patients(cursor):
    """
    Create a function called map_orders_to_patients that takes in patients data and
    procedure orders and returns number of orders per each patient
    """
    sqlite3_query_tmplt = f"select pa.encounter_id, count(procedure_id) as procedure_orders from procedure_order as pr join patient_data as pa  on pr.encounter_id = pa.encounter_id group by pa.encounter_id;"
    print(f"SQLITE query: {sqlite3_query_tmplt}")
    total_result = list(cursor.execute(sqlite3_query_tmplt))
    # clean results
    transformed_r = list(map(lambda r: {'encounter_id':r[0], 'procedure_orders': r[1]}, total_result))
    return transformed_r


def _patients_with_procedure(search_pattern, cursor):
    sqlite3_query_tmplt = f"""select pa.encounter_id, count(procedure_id) as procedure_orders from procedure_order as pr join patient_data as pa on pr.encounter_id = pa.encounter_id 
where pr.id IN (select csv_id from procedure_text_search WHERE procedure_name MATCH '{search_pattern}')
group by pa.encounter_id;"""
    print(f"SQLITE query: {sqlite3_query_tmplt}")
    total_result = list(cursor.execute(sqlite3_query_tmplt))
    # clean results
    transformed_r = list(map(lambda r: {'encounter_id':r[0], 'procedure_orders': r[1]}, total_result))
    return transformed_r



def main():
    # # Connecting to the geeks database
    db_file_path = os.path.join('/tmp/part2_qventus.db')
    procedure_rows = read_procedure_csv()
    patients_rows = read_patient_csv()
    search_pattern = "hospitalist*"
    with sqlite3.connect(db_file_path) as connection:
        cursor = connection.cursor()
        # Part 2 - Question 1
        prepate_procedures_text_search_db(procedure_rows, cursor, connection)
        r = _search_procedure_orders(search_pattern=search_pattern, cursor=cursor)
        print("\n\nPART 2 - QUESTION 1")
        print("=== search_procedure_orders ===")
        print(f"Search pattern = '{search_pattern}'")
        print("--- result set -----")
        print(r)
        # Part 2 - Question 2
        print("\n\nPART 2 - QUESTION 2")
        print("=== map_orders_to_patients ===")
        create_db_tables(cursor)
        insert_procedure_rows(cursor, connection, procedure_rows)
        insert_patient_rows(cursor, connection, patients_rows)
        r = _map_orders_to_patients(cursor)
        print("--- result set -----")
        print(r)
        # Part 2 - Question 3
        print("\n\nPART 2 - QUESTION 3")
        print("=== patients_with_procedure ===")
        print(f"Search pattern = '{search_pattern}'")
        r = _patients_with_procedure(search_pattern=search_pattern, cursor=cursor)
        print("--- result set -----")
        print(r)


if __name__ == '__main__':
    main()
