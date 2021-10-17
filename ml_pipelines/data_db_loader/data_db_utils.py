import csv
import logging
from itertools import chain
from typing import List, Tuple, Any

import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq

logging.getLogger().setLevel(logging.INFO)


def generate_create_query(column_definitions: List[Tuple[str, str]],
    dbname: str,
    tablename: str) -> str:
    """Generate query to create a table in a database

    Args:
        column_definitions (List[Tuple[str, str]]): list of (column, type) pairs
        dbname (str): database name hosting the table
        tablename (str): name of the table to be created

    Returns:
        str: Query which can be run to create the table
    """
    create_line = f"CREATE TABLE IF NOT EXISTS {dbname}.{tablename}"

    column_lines = [f"{col[0]} {col[1]}" for col in column_definitions]
    column_lines = ",\n".join(column_lines)

    create_query = f"{create_line} (\n{column_lines}\n)"

    return create_query


def create_table(cnx: mysql.connector.connection.MySQLConnection,
    dbname: str,
    tablename: str,
    create_query: str) -> None:
    """Create a table in a database with a CREATE query

    Args:
        cnx (mysql.connector.connection.MySQLConnection): MySQL server connection
        dbname (str): database name hosting the table
        tablename (str): name of the table to be created
        create_query (str): query used to create table <tablename> in DB <dbname>
    """
    mycursor = cnx.cursor()
    mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    mycursor.execute(create_query)
    mycursor.execute(f"TRUNCATE TABLE {dbname}.{tablename}")
    cnx.commit()
    mycursor.close()


def generate_insert_query(column_definitions: List[Tuple[str, str]],
    dbname: str,
    tablename: str) -> str:
    """Generate a query to insert data into a table

    Args:
        column_definitions (List[Tuple[str, str]]): list of (column, type) pairs
        dbname (str): database name hosting the table
        tablename (str): name of the table to insert data into

    Returns:
        str: Query which can be run to insert data into the table
    """
    insert_line = f"INSERT INTO {dbname}.{tablename}"

    column_names = [col[0] for col in column_definitions]
    column_names = ", ".join(column_names)
    column_line = f"({column_names})"

    value_placeholder = "%s"
    number_of_columns = len(column_definitions)
    values = ", ".join([value_placeholder] * number_of_columns)
    values_line = f"({values})"

    insert_query = f"{insert_line}\n{column_line}\nVALUES\n{values_line}"

    return insert_query


def insert_data_from_csv(cnx: mysql.connector.connection.MySQLConnection,
    datafilename: str,
    delimiter: str,
    insert_query: str,
    batch_size: int) -> None:
    """Copy data from a .csv file into a MySQL table (data is sent in batches)

    Args:
        cnx (mysql.connector.connection.MySQLConnection): MySQL server connection
        datafilename (str): the .csv file with data to be copied into MySQL
        delimiter (str): field delimiter in the .csv file (tab, comma, etc.)
        insert_query (str): query used for inserting the data
        batch_size (int): number of rows to be sent to database at once
    """
    logging.info(f"Sending data from {datafilename} to database...")
    with open(datafilename, "r") as file:
        data_reader = csv.reader(file, delimiter=delimiter)
        # skip header
        next(data_reader)
        mycursor = cnx.cursor()
        batch_number = 0
        while True:
            batch_number += 1
            logging.info(f"Inserting batch number {batch_number}...")
            try:
                current_batch = []
                for _ in range(batch_size):
                    current_batch.append(next(data_reader))
                    current_batch[-1] = [val if val not in ("NONE", "NULL") else None 
                        for val in current_batch[-1]]
                mycursor.executemany(insert_query, current_batch)
            except StopIteration:
                break
    if current_batch:
        mycursor.executemany(insert_query, current_batch)
    logging.info("Committing changes...")
    cnx.commit()
    mycursor.close()
    logging.info("Inserting data done.")


def iter_parquet(file_path: str, columns = None, batch_size=50000) -> Tuple[Any]:
    """Get data from large parquet files in batches.

    Args:
        file_path (str): [description]
        columns ([type], optional): [description]. Defaults to None.
        batch_size (int, optional): [description]. Defaults to 3.

    Returns:
        Tuple[Any]: [description]

    Yields:
        Iterator[Tuple[Any]]: [description]
    
    Usage examples
    ______________
    1.
    >>> parquet_reader = iter_parquet("data/backend_test_data/recoms")
    >>> next_batch = next(parquet_reader)
    2.
    >>> parquet_reader = iter_parquet("data/backend_test_data/recoms")
    >>> for i, j in enumerate(parquet_reader):
    >>>     print("batch", i + 1)
    >>>     print(j)
    """

    # # create file system for file interface objects from S3
    # fs = s3fs.S3FileSystem()

    # open a file interface object
    # with fs.open(s3_uri) as fp:
    with open(file_path, "rb") as fp:

        # convert the python file object into a ParquetFile object for iterating
        parquet_file = pq.ParquetFile(fp)

        # an iterator of pyarrow.RecordBatch
        record_batches = parquet_file.iter_batches(batch_size=batch_size, columns=columns)

        # convert from columnar format of pyarrow arrays to a row format of python objects (yields tuples)
        yield from chain.from_iterable(zip(*map(lambda col: col.to_pylist(), batch.columns)) for batch in record_batches)
