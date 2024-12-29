Notes:
1. If DB is already filled with data, or some data, "Error importing data for table..." will print; 
this message just warns of failure to fill in data with already existing, UNIQUE values.

2. Ensure the following directory contents for proper program execution: 
    pwd -> load_data.py, CP_DSSTox_schema_script.sql, Data directory, Data_Sample directory.

3. The Data directory is empty intentionally (to preseve space); import all data files in Data directory.

How to Run the Script: 
To run the script, use the following command (ensure all files are included, and consider their respective directory for proper execution):
Ex. 1: python3 load_data.py --db CP_DSSTox_.db --schema CP_DSSTox_schema_script.sql --data ./Data --TEST no
Ex. 2: python3 load_data.py --db CP_DSSTox_test.db --schema CP_DSSTox_schema_script.sql --data ./Data_Sample --TEST yes

--db is an arbitrary database.
--schema is the CP_DSSTox schema sql file.
--data is the Data directory containing CSV and Excel files.
--TEST is a yes/no argument to execute a test version of the program (if yes, note that the Data directory must contain appropriate sample data).