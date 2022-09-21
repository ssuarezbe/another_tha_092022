#!/usr/bin/python3
import csv
import pathlib
import sqlite3
import os
from datetime import datetime 

current_path = pathlib.Path(__file__).parent.resolve()

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


 
def create_table(cursor):
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


def read_patient_csv():
    """
    Read csv data and parsed the data so is valid with Sqlite table schema
    """
    file_path = os.path.join(current_path,'../../data','qventus_interview_patient_data_dataset.csv')
    rows = []
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
            rows.append(parsed_row)
    if rows_count != len(rows):
        raise ValueError("Rows count mismatch {}=!{} after reading file qventus_interview_patient_data_dataset.csv".format(rows_count, len(rows)))
    return rows


def read_procedure_csv():
    """
    Read csv data and parsed the data so is valid with Sqlite table schema
    """
    file_path = os.path.join(current_path,'../../data','qvenus_interview_procedure_orders.csv')
    rows = []
    date_fmt ="%Y-%m-%d %H:%M:%S"
    rows_count = 0
    with open(file_path, 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            rows_count += 1;
            d = datetime.strptime(row[3], date_fmt)
            parsed_row = (int(row[0]), int(row[1]), int(row[2]), d, row[4], row[5], row[6])
            rows.append(parsed_row)
    if rows_count != len(rows):
        raise ValueError("Rows count mismatch {}=!{} after reading file qvenus_interview_procedure_orders.csv".format(rows_count, len(rows)))
    return rows


def main():
    
    # # Connecting to the geeks database
    db_file_path = os.path.join('/tmp/local.db')
     
    # # Creating a cursor object to execute
    # # SQL queries on a database table
    with sqlite3.connect(db_file_path) as connection:
        cursor = connection.cursor()
        create_table(cursor)
        # read procedure csv
        procedure_rows = read_procedure_csv()
        insert_procedure_rows(cursor, connection, tuple(procedure_rows))
        # read patients csv
        patients_rows = read_patient_csv()
        insert_patient_rows(cursor, connection, tuple(patients_rows))




if __name__ == '__main__':
    main()
