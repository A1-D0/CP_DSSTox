'''
Description: This program gets CP CSV and DSSTox XLSX files, reads their data, and appropriately imports such data into the CP_DSSTox database.
Author: Osvaldo Hernandez-Segura
References: ChatGPT
Dependencies: pandas, sqlite3, argparse, os, datetime, collections.defaultdict
'''

import pandas as pd
import sqlite3 as sql
import argparse
import os
from datetime import datetime
from collections import defaultdict

def replace_empty_na_with_null(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Replaces empty strings or 'NA' values in the DataFrame with None (SQL NULL equivalent).

    Arguments:
        df (pd.DataFrame): DataFrame with potential empty or 'NA' values.

    Returns:
        pd.DataFrame: DataFrame with empty and 'NA' values replaced by None.
    '''
    return df.replace(['', 'NA'], None)

def import_data_to_DB(db_path: str, data: dict) -> None:
    '''
    Imports data from DataFrames into SQLite database.

    Arguments:
        db_path (str): Path to SQLite database file.
        data (dict): Dictionary where keys are table names and values are lists of DataFrames to import.

    Returns:
        None
    '''
    with sql.connect(db_path) as conn:
        for table_name, df_list in data.items(): # iterate through each table name and its corresponding DataFrames
            try:
                for df in df_list: # iterate through each DataFrame for current table (Cf. DSSTox)
                    df = replace_empty_na_with_null(df)
                    import_table_data(conn, table_name, df)
            except Exception as e: 
                print(f"Error importing data for table '{table_name}': {e}")

def explode_identifiers(df: pd.DataFrame):
    '''
    Splits IDENTIFIER column in DSSTox data into multiple rows for Identifier table.

    Arguments:
        df (pd.DataFrame): DataFrame containing DSSTox data with an 'IDENTIFIER' column.

    Returns:
        tuple: Updated DSSTox DataFrame with 'IDENTIFIER' column modified, and DataFrame for the Identifier table.
    '''
    identifier_rows = []
    for _, row in df.iterrows(): # iterate through each row in DataFrame
        identifiers = str(row['IDENTIFIER']).split('|')
        casrn = identifiers[0].strip()
        for alt_id in identifiers[1:]: # iterate through alternative identifiers after first
            identifier_rows.append({'IDENTIFIER': casrn, 'CASRN': casrn, 'ALTERNATIVE_IDENTIFIER': alt_id.strip()})
    identifier_df = pd.DataFrame(identifier_rows)
    # update DSSTox DataFrame to have only first identifier
    df['IDENTIFIER'] = df['IDENTIFIER'].apply(lambda x: x.split('|')[0].strip())
    return df, identifier_df

def Identifier_import_data(conn, cursor, identifier_df: pd.DataFrame) -> None:
    '''
    Imports data into the Identifier table from the exploded identifiers.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        identifier_df (pd.DataFrame): DataFrame containing data for the Identifier table.

    Returns:
        None
    '''
    for _, row in identifier_df.iterrows(): # insert into Identifier table
        cursor.execute('''
            INSERT INTO Identifier (IDENTIFIER, CASRN, ALTERNATIVE_IDENTIFIER)
            VALUES (?, ?, ?)
        ''', (row['IDENTIFIER'], row['CASRN'], row['ALTERNATIVE_IDENTIFIER']))
    conn.commit()
    cursor.close()

def DSSTox_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the DSSTox table in the SQLite database using parameterized SQL insert statements.
    Also calls the Identifier_import_data function to insert data into the Identifier table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    # handle specific processing for DSSTox table
    dsstox_df, identifier_df = explode_identifiers(df)
    dsstox_df = dsstox_df.where(pd.notnull(dsstox_df), None)
    identifier_df = identifier_df.where(pd.notnull(identifier_df), None)
    # insert into DSSTox table
    for _, row in dsstox_df.iterrows():
        cursor.execute('''
            INSERT INTO DSSTox (DTXSID, PREFERRED_NAME, CASRN, INCHIKEY, IUPAC_NAME, SMILES,
                                MOLECULAR_FORMULA, AVERAGE_MASS, MONOISOTOPIC_MASS,
                                QSAR_READY_SMILES, MS_READY_SMILES, IDENTIFIER)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    Identifier_import_data(conn, cursor, identifier_df)
    conn.commit()
    cursor.close()

def chemical_dictionary_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports data into the chemical_dictionary table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO chemical_dictionary (chemical_id, raw_chem_name, raw_casrn, preferred_name, preferred_casrn, DTXSID, curation_level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def QSUR_data_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports data into the QSUR_data table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute(f'''
            INSERT INTO QSUR_data (DTXSID, preferred_name, preferred_casrn, harmonized_function, probability)
            VALUES (?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def parse_date(date_str: str) -> str:
    '''
    Parses a date string into YYYY-MM-DD format.

    Arguments:
        date_str (str): Date string to parse.

    Returns:
        str or None: Parsed date string in 'YYYY-MM-DD' format, or None if parsing fails.
    '''
    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%d-%b-%y", "%d-%b-%Y", "%d-%B-%Y", "%B-%y", "%b-%y", "%B %d, %Y", "%B %Y", "%Y", "%d/%m/%Y", "%m/%d/%Y", "%d %B %Y", "%m.%d.%Y"]
    for fmt in date_formats: # iterate through possible date formats
        try: # try to parse date string with current format
            newDate = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            return newDate
        except (ValueError, AttributeError):
            continue
    return None # if parsing fails

def convert_dates(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    '''
    Converts dates in various formats to YYYY-MM-DD format.

    Arguments:
        df (pd.DataFrame): DataFrame containing the date column to convert.
        date_column (str): Name of the date column in the DataFrame.

    Returns:
        pd.DataFrame: DataFrame with the date column converted to standardized format.
    '''
    # apply date parsing to specified date column
    df[date_column] = df[date_column].apply(lambda x: parse_date(x) if pd.notnull(x) else None)
    return df

def document_dictionary_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into a document_dictionary table in the SQLite database using parameterized SQL insert statements.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    # handle specific processing for document_dictionary table
    df = convert_dates(df, "doc_date")
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO document_dictionary (document_id, title, subtitle, doc_date)
            VALUES (?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def to_float(value) -> float:
    '''
    Converts a value to float, handling percentages.

    Arguments:
        value (str or numeric): Value to convert to float.

    Returns:
        float or None: Converted float value, or None if conversion fails.
    '''
    try:
        if pd.notnull(value):
            value_str = str(value).strip()
            if '%' in value_str:
                # remove percentage sign and convert to float
                value_str = value_str.replace('%', '')
                float_value = float(value_str) / 100.0
            else:
                float_value = float(value_str)
            return float_value
        return None
    except ValueError:
        return None

def convert_compositions(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Converts composition columns to floats, handling percentages and nulls.

    Arguments:
        df (pd.DataFrame): DataFrame containing composition columns to convert.

    Returns:
        pd.DataFrame: DataFrame with composition columns converted to floats.
    '''
    composition_columns = ['raw_min_comp', 'raw_central_comp', 'raw_max_comp']
    # iterate through each composition column to convert values
    for col in composition_columns:
        df[col] = df[col].apply(lambda x: to_float(x))
    return df

def product_composition_data_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into a product_composition_data table in the SQLite database using parameterized SQL insert statements.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None.
    '''
    # handle specific processing for product_composition_data table
    df = convert_compositions(df)
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO product_composition_data (document_id, product_id, chemical_id, functional_use_id, puc_id, classification, prod_title, brand_name, raw_min_comp, raw_central_comp, raw_max_comp, clean_min_wf, clean_central_wf, clean_max_wf)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()   

def list_presence_dictionary_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the list_presence_dictionary table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO list_presence_dictionary (list_presence_id, name, definition, kind)
            VALUES (?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def list_presence_data_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the list_presence_data table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO list_presence_data (document_id, chemical_id, list_presence_id)
            VALUES (?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def HHE_data_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the HHE_data table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO HHE_data (document_id, chemical_id)
            VALUES (?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def functional_use_dictionary_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the functional_use_dictionary table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO functional_use_dictionary (chemical_id, functional_use_id, report_funcuse, oecd_function)
            VALUES (?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def functional_use_data_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the functional_use_data table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO functional_use_data (document_id, chemical_id, functional_use_id)
            VALUES (?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def PUC_dictionary_import_data(conn, cursor, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into the PUC_dictionary table.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        cursor (sqlite3.Cursor): Cursor for executing SQL commands.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO PUC_dictionary (puc_id, gen_cat, prod_fam, prod_type, description, puc_code, kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))
    conn.commit()
    cursor.close()

def import_table_data(conn, table_name: str, df: pd.DataFrame) -> None:
    '''
    Imports a DataFrame into a specific table in the SQLite database using parameterized SQL insert statements.

    Arguments:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table to insert data into.
        df (pd.DataFrame): DataFrame containing data to insert.

    Returns:
        None
    '''
    cursor = conn.cursor()
    # ordered for foreign key constraint and dependency compliance
    if table_name == "document_dictionary":
        document_dictionary_import_data(conn, cursor, df)
    elif table_name == "chemical_dictionary":
        chemical_dictionary_import_data(conn, cursor, df)
    elif table_name == "list_presence_dictionary":
        list_presence_dictionary_import_data(conn, cursor, df)
    elif table_name == "PUC_dictionary":
        PUC_dictionary_import_data(conn, cursor, df)
    elif table_name == "functional_use_dictionary":
        functional_use_dictionary_import_data(conn, cursor, df)
    elif table_name == "DSSTox":
        DSSTox_import_data(conn, cursor, df)
    elif table_name == "QSUR_data":
        QSUR_data_import_data(conn, cursor, df)
    elif table_name == "functional_use_data":
        functional_use_data_import_data(conn, cursor, df)
    elif table_name == "product_composition_data":
        product_composition_data_import_data(conn, cursor, df)
    elif table_name == "list_presence_data":
        list_presence_data_import_data(conn, cursor, df)
    elif table_name == "HHE_data":
        HHE_data_import_data(conn, cursor, df)
    else: # terminate if none exist
        print(f"Data for table '{table_name}' has not been imported successfully.")
        exit(1)
    print(f"Data for table '{table_name}' has been imported successfully.")

def extract_data(file_paths: dict) -> dict:
    '''
    Reads data from Excel and CSV files into pandas DataFrames.
    Terminates program if any data extraction of a file_path is empty.

    Arguments:
        file_paths (dict): Dictionary where keys are table names and values are lists of file paths.

    Returns:
        dict: Dictionary where keys are table names and values are lists of DataFrames.
    '''
    data = {}
    for table_name, paths in file_paths.items(): # iterate through each table name and its corresponding file paths
        df_list = []
        for file_path in paths: # iterate through each file path for current table (Cf. DSSTox files)
            df = load_file(file_path)
            if df.empty: # terminate if any data extraction of a file_path is empty
                exit(1)
            df_list.append(df)
        data[table_name] = df_list
    return data

def parse_file_paths(data_dir: str) -> dict:
    '''
    Defines the list of file paths to import for each table.
    Terminates program if there is a missing required CSV or Excel file.
    
    Arguments:
        data_dir (str): Directory name hosting CSV and Excel files for this program.

    Returns:
        dict: Dictionary where keys are table names and values are lists of file paths.
    '''
    file_paths = defaultdict(list)
    
    # DSSTox files
    for i in range(1, 14):
        file_name = f"{data_dir}/DSSToxDump{i}.xlsx"
        file_paths["DSSTox"].append(file_name)

    # file paths to corresponding relations
    file_paths["document_dictionary"].append(f"{data_dir}/document_dictionary_20201216.csv")
    file_paths["chemical_dictionary"].append(f"{data_dir}/chemical_dictionary_20201216.csv")
    file_paths["list_presence_data"].append(f"{data_dir}/list_presence_data_20201216.csv")
    file_paths["PUC_dictionary"].append(f"{data_dir}/PUC_dictionary_20201216.csv")
    file_paths["functional_use_dictionary"].append(f"{data_dir}/functional_use_dictionary_20201216.csv")
    file_paths["QSUR_data"].append(f"{data_dir}/QSUR_data_20201216.csv")
    file_paths["functional_use_data"].append(f"{data_dir}/functional_use_data_20201216.csv")
    file_paths["product_composition_data"].append(f"{data_dir}/product_composition_data_20201216.csv")
    file_paths["list_presence_dictionary"].append(f"{data_dir}/list_presence_dictionary_20201216.csv")
    file_paths["HHE_data"].append(f"{data_dir}/HHE_data_20201216.csv")
    
    # verify that files exist
    for table_name, paths in list(file_paths.items()):
        for file_path in paths:
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                paths.remove(file_path)
        if not paths: # Data dir fails to have all data
            exit(1)
    return file_paths

def load_file(file_path: str) -> pd.DataFrame:
    '''
    Loads data from the given file path into a DataFrame.
    Terminates the program if a file format is not accepted or unable to be read.

    Arguments:
        file_path (str): Path to the file to load.

    Returns:
        pd.DataFrame: DataFrame containing the loaded data.
    '''
    try:
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'): # read Excel file into DataFrame
            return pd.read_excel(file_path)
        elif file_path.endswith('.csv'): # attempt to read CSV with different encodings
            for encoding in ['utf-8', 'ISO-8859-1', 'Windows-1252']:
                try:
                    return pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    print(f"Encoding {encoding} failed for {file_path}. Trying next encoding.")
            print(f"All encodings failed for {file_path}. Exiting.")
            exit(1)
        else: # terminate if there are any unsupported file formats
            print(f"Unsupported file format for {file_path}")
            exit(1)
    except Exception as e: # terminate if file reading error
        print(f"Error reading file {file_path}: {e}")
        exit(1)

def create_database(db_path: str, schema_file: str) -> None:
    '''
    Creates SQLite database using provided schema file.

    Arguments:
        db_path (str): Path to SQLite database file.
        schema_file (str): Path to SQL schema file.

    Returns:
        None
    '''
    with sql.connect(db_path) as conn:
        # open schema file and read SQL commands
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
            cursor = conn.cursor()
            # execute schema SQL script to create tables
            cursor.executescript(schema_sql)
    print(f"Database created successfully at {db_path}")

def validate_file_paths(paths) -> None:
    """
    Validates that each path in the list exists. Exits with an error message if any path is missing.

    Args:
        paths (list of str): List of file paths to validate.

    Returns:
        None
    """
    for path in paths:
        if not os.path.exists(path):
            print(f"Error: File not found - {path}")
            exit(1)

def parse_file_paths_for_test(data_dir: str) -> dict:
    '''
    Defines the list of file paths to import for each table for testing.
    Terminates program if there is a missing required CSV or Excel file.
    
    Arguments:
        data_dir (str): Directory name hosting CSV and Excel files for this program.

    Returns:
        dict: Dictionary where keys are table names and values are lists of file paths.
    '''
    file_paths = defaultdict(list)
    
    # file paths to corresponding relations
    file_paths["DSSTox"].append(f"{data_dir}/DSSTox_sample.xlsx")
    file_paths["document_dictionary"].append(f"{data_dir}/document_dictionary_sample.csv")
    file_paths["chemical_dictionary"].append(f"{data_dir}/chemical_dictionary_sample.csv")
    file_paths["list_presence_dictionary"].append(f"{data_dir}/list_presence_dictionary_sample.csv")
    file_paths["PUC_dictionary"].append(f"{data_dir}/PUC_dictionary_sample.csv")
    file_paths["functional_use_dictionary"].append(f"{data_dir}/functional_use_dictionary_sample.csv")
    file_paths["QSUR_data"].append(f"{data_dir}/QSUR_data_sample.csv")
    file_paths["functional_use_data"].append(f"{data_dir}/functional_use_data_sample.csv")
    file_paths["product_composition_data"].append(f"{data_dir}/product_composition_data_sample.csv")
    file_paths["list_presence_data"].append(f"{data_dir}/list_presence_data_sample.csv")
    file_paths["HHE_data"].append(f"{data_dir}/HHE_data_sample.csv")

    # verify that files exist
    for table_name, paths in list(file_paths.items()):
        for file_path in paths:
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                paths.remove(file_path)
        if not paths: # Data dir fails to have all data
            exit(1)
    return file_paths

def test_program(data_dir: str, db_path: str, schema_file: str) -> None:
    '''
    Tests the data import functionality using sample data for the original schema.

    Arguments:
        data_dir (str): Directory with sample data files.
        db_path (str): Path to SQLite database file.
        schema_file (str): Path to SQL schema file.

    Returns:
        None
    '''
    print("Testing...")
    data_files = parse_file_paths_for_test(data_dir)
    
    # create database using provided schema
    create_database(db_path, schema_file)

    # extract data from files
    data = extract_data(data_files)

    # attempt to import data into database
    if data:
        import_data_to_DB(db_path, data)
        print("Data import process completed successfully.")
    else:
        print("No valid data to import!")

def main() -> None: # Given that we are in the Code directory...
    parser = argparse.ArgumentParser(description="Load data from Excel and CSV files into an SQLite database.")
    parser.add_argument('--db', required=True, type=str, help="Path to the SQLite database file.")
    parser.add_argument('--schema', required=True, type=str, help="Path to the SQL schema file.")
    parser.add_argument('--data', required=True, type=str, help="Path to the data: all Excel and CSV files.")
    parser.add_argument('--TEST', required=True, type=str, help="Test sample Excel and CSV files (yes/no).")

    # parse arguments
    args = parser.parse_args()
    
    # define database and schema paths
    db_path = args.db
    schema_file = args.schema
    data_path = args.data
    test_bool = args.TEST == "yes" # check for testing intent

    if test_bool: # enter test state
        test_program(data_path, db_path, schema_file)
        print("Program complete!")
        exit(0)

    # parse files in data dir
    data_files = parse_file_paths(data_path)

    # create database using provided schema
    create_database(db_path, schema_file)

    # extract data from files
    data = extract_data(data_files)
    
    # attempt to import data into database
    if data:
        import_data_to_DB(db_path, data)
        print("Data import process completed successfully.")
    else:
        print("No valid data to import!")

    exit(0)

if __name__ == "__main__":
    main()