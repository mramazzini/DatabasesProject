import psycopg2

# Define the database connection parameters
conn_params = {
    'dbname': 'hw1',        # Your database name
    'user': 'postgres',  # Your PostgreSQL username
    'password': 'a',  # Your PostgreSQL password
    'host': 'localhost',    # Host, usually 'localhost'
    'port': 5432            # Default PostgreSQL port
}

def execute_sql_file(filename):
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Open and read the SQL file
        with open(filename, 'r') as file:
            sql_queries = file.read()

        # Execute the SQL commands from the file
        cursor.execute(sql_queries)

        # Commit the changes (important for data-modifying queries)
        conn.commit()

        print("SQL file executed successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def execute_sql_query(query):
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch and print the result
        result = cursor.fetchall()
        return result

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def parseTxtFile(filename):
    tables = []
    with open(filename, 'r') as file:
        lines = file.readlines()
        tableData = {}
        for line in lines:
            tableData = {}
            tableData["name"] = line.split('(')[0]
            columnStrings = line.split(tableData["name"])[1].replace('\n', '')[1:-1].split(',')

            for columnString in columnStrings:
                # This is a column with a foreign or primary key
                col = {}
                if columnString[len(columnString)-1] == ')':
                    columnString = columnString[:-1]
                    columnName = columnString.split('(')[0]
                    keyData = columnString.split('(')[1]
                    if (keyData[:2] == 'pk'):
                        col["key"] = "pk"
                    else:
                        col["key"] = "fk"
                        keyData = keyData.split(':')[1].split('.')
                        col["table"] = keyData[0]
                        col["column"] = keyData[1]
                    tableData[columnName] = col
                    tableData[columnName]["data"] = []
                else:
                    columnName = columnString
                    col["key"] = "none"
                    tableData[columnName] = col
                    tableData[columnName]["data"] = []
            tables.append(tableData)
    for table in tables:
        data = execute_sql_query(f"SELECT * FROM {table['name']}")
        for row in data:
            colIndex = 0
            for col in table:
                if col == "name":
                    continue
                colData = table[col]
                for key in colData:
                    if key == "data":
                        if row[colIndex] == None:
                            colData[key].append("NULL")
                        else:
                            colData[key].append(row[colIndex])
                colIndex += 1
   
                
    return tables
                    
def referentialIntegrityCheck(tables):
    # check for columns that use foriegn keys, and check if the column they reference exists

    integrity_results = {}


    for table in tables:
        for column in table:
            if column == 'name':
                continue
            if table[column]["key"] == "fk":
                # check if the column exists
                for tableToCheck in tables:
                    if tableToCheck["name"] == table[column]["table"]:
                            integrity_results[table["name"]] =  "Y"
                            break
                    else:
                        integrity_results[table["name"]] = "N"



    return integrity_results ## returns dict full of keys with there RIC results

def format_output(filename, lines):
    # Print filename
    print(f"{filename}")
    print("-" * 41)
    # Print header
    print(f"{'Table':<10} {'referential':^15} {'normalized':^15}")
    print(f"{'':<10} {'integrity':^15} {'':<15}")
    print("-" * 41)
    # Process table data
    for line in lines:
        if line.strip(): # Skip empty lines
            parts = line.strip().split()
            if len(parts) == 3:
                print(f"{parts[0]:<10} {parts[1]:^15} {parts[2]:^15}")
    # Process DB summary
    db_ref_integrity = "Y" if all(line.split()[1] == "Y" for line in lines if line.strip()) else "N"
    db_normalized = "Y" if all(line.split()[2] == "Y" for line in lines if line.strip()) else "N"
    # print("-" * 41)
    print(f"{'DB referential integrity:':<30} {db_ref_integrity}")
    print(f"{'DB normalized:':<30} {db_normalized}")

# normalizationCheck(tables):
#   normalization_results = {}

#   for each table in tables:
#      normalization_results[table["name"]] = "Normalized"

#       if hasRepeatingGroups(table) or hasPartialDependencies(table) or hasTransitiveDependencies(table):
#           normalization_results[table["name"]] = "Not Normalized"

#   return normalization_results


# Specify the path to your SQL file
sqlFilePaths = ['tc1.sql', 'tc2.sql', 'tc3.sql', 'tc4.sql', 'tc5.sql']
txtFilePaths = ['tc1.txt', 'tc2.txt', 'tc3.txt', 'tc4.txt', 'tc5.txt']


# Call the function to execute the SQL file

for i in range(0,5):
    execute_sql_file(sqlFilePaths[i])

    tableData = parseTxtFile(txtFilePaths[i])

    # for table in tableData:
    #     print(table)

    referentialIntegrityCheckResults = referentialIntegrityCheck(tableData)

    lines = []

    for table in referentialIntegrityCheckResults:
        lines.append(f"{table} {referentialIntegrityCheckResults[table]} Y")

    format_output(txtFilePaths[i],lines)