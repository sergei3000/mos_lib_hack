from typing import List, Tuple

import mysql.connector

from data_db_utils import (generate_create_query,
    generate_insert_query,
    create_table,
    insert_data_from_csv)

DBPARAMS = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    # "password": "password",
    }
DB_NAME = "mos_lib_hack"
RECOMMENDATIONS_TABLE = "recommendations"
HISTORY_TABLE = "history"
RECOMMENDATIONS_COLUMN_DEFINITIONS = [
    ("user_id", "bigint"),
    ("item_id", "bigint"),
    ("title", "varchar(128)"),
    ("author", "varchar(128)"),
    ("ranking", "int"),
]
HISTORY_COLUMN_DEFINITIONS = [
    ("user_id", "bigint"),
    ("item_id", "bigint"),
    ("title", "varchar(128)"),
    ("author", "varchar(128)"),
]
CSV_FILE_DELIMITER = ","
# How many rows to push through connection at once
BATCH_SIZE = 50000


def send_to_database(datafilename: str,
    dbname: str,
    tablename: str,
    column_definitions: List[Tuple[str, str]]) -> None:
    """Take the data file, and store it as a table on the database
    server.

    """
    cnx = mysql.connector.connect(**DBPARAMS)

    create_table_query = generate_create_query(column_definitions, dbname, tablename)
    create_table(cnx, dbname, tablename, create_table_query)

    insert_query = generate_insert_query(column_definitions, dbname, tablename)
    insert_data_from_csv(cnx, datafilename, CSV_FILE_DELIMITER, insert_query, int(BATCH_SIZE))

    cnx.close()


def main():
    data_dir = "../../data/" if __name__ == "__main__" else "data/"
    recommendations_data = f"{data_dir}recommendations.csv"
    history_data = f"{data_dir}history.csv"
    
    send_to_database(recommendations_data,
        DB_NAME,
        RECOMMENDATIONS_TABLE,
        RECOMMENDATIONS_COLUMN_DEFINITIONS)
    
    send_to_database(history_data,
        DB_NAME,
        HISTORY_TABLE,
        HISTORY_COLUMN_DEFINITIONS)


if __name__ == "__main__":
    main()
