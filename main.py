# Test code to create database and read multiple CSV files into the table
# for test searches
# This code reads every CSV file located in the hard-coded directory into a SQLite database file
# The resulting database will contain 1 row for every day of each stock symbol in the time period
# which results in a very large file with stock symbols repeated many times.
# The intent of this DB is to use it to sort out unique symbols and then create new databases
# with the sorted and parsed data
# Note that the CSVs have some inconsistent date formats which will cause issues for sorting by date
# Also note that each CSV has a header with the column titles.  For each CSV that's read into the DB
# there will be duplicate columns with these column titles.  To be sorted in subsequent database files
# There is an assumption that each CSV has consistent format... this is not checked in this code

# This script is intended to be run one time only to create the initial database of all CSVs combined

import sqlite3
import csv
import os

connection = sqlite3.connect('../../Data_For_Stock_Project/all_stocks_data.db')
cursor = connection.cursor()
cursor.execute("""
CREATE TABLE if not exists stocks (
    Symbol TEXT,
    Date,
    Open,
    High,
    Low,
    Close,
    Volume INTEGER)
""")
# Note that if 'y' is selected when the database is already partially populated then
# duplicate rows will likely be created.  If you keep track of which CSVs have already been
# captured then you can run this multiple times and select 'y' for new CSVs.  But this is not
# recommended since it leaves room for error and duplication which could lead to erroneous analysis
q = input('Do you want to insert data during this run? (y/n)')
a = 0
if q == 'y':
    path = '../../Data_For_Stock_Project/Stock_Data_CSVs'
    files = os.listdir(path)
    for csv_name in files:
        a = a + 1
        csv_path = path + '/' + csv_name
        with open(csv_path) as f:
            reader = csv.reader(f)
            cursor.executemany("""
                INSERT INTO stocks VALUES (?,?,?,?,?,?,?)
                """, reader)
    print(a)
    connection.commit()

cursor.execute("SELECT * FROM stocks")
results = cursor.fetchall()

print(len(results))


connection.commit()