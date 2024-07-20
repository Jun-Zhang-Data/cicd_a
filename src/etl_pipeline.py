import pandas as pd
import snowflake.connector
import yaml


def get_snowflake_connection(config):
    """Establish a connection to Snowflake."""
    conn = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    return conn

def setup_environment(conn):
    """Set up the Snowflake environment."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE WAREHOUSE {conn.warehouse};")
        cursor.execute(f"USE DATABASE {conn.database};")
        cursor.execute(f"USE SCHEMA {conn.schema};")
        print("Environment set up successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"SQL error: {e}")
    finally:
        cursor.close()

def create_and_insert_tables(config):
    """Create a table, insert values, and create another table in Snowflake."""
    conn = get_snowflake_connection(config)
    cursor = conn.cursor()

    # SQL command to create the first table
    create_table_1_sql = """
    CREATE OR REPLACE TABLE my_table (
        id INT,
        name STRING,
        age INT
    );
    """

    # SQL command to insert values into the first table
    insert_values_sql = """
    INSERT INTO my_table (id, name, age) VALUES 
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35);
    """

    # SQL command to create the second table
    create_table_2_sql = """
    CREATE OR REPLACE TABLE my_table_transformed (
        id INT,
        name STRING,
        age INT,
        age_plus_ten INT
    );
    """

    try:
        # Execute SQL commands
        cursor.execute(create_table_1_sql)
        cursor.execute(insert_values_sql)
        cursor.execute(create_table_2_sql)
        
        print("Tables created and values inserted successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"SQL error: {e}")
    finally:
        cursor.close()
        conn.close()


def extract_data(query, config):
    conn = get_snowflake_connection(config)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return pd.DataFrame(data, columns=columns)

def transform_data(df):

    print(df.columns)
    """
    Perform transformations on the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    if 'AGE' not in df.columns:
        raise KeyError("'age' column is missing from DataFrame")
    
    # Add a new column 'age_plus_ten' by adding 10 to the 'age' column
    df['age_plus_ten'] = df['AGE'] + 10
    
    return df


def load_data(df, table_name, config):
    conn = get_snowflake_connection(config)
    cursor = conn.cursor()

    try:
        # Clear the table before inserting new data
        cursor.execute(f"DELETE FROM {table_name};")

    
        for _, row in df.iterrows():
            columns = ", ".join(df.columns)
            values = "', '".join(str(v) for v in row)
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ('{values}')")
            
        print(f"Data loaded successfully into {table_name}.")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"SQL error: {e}")
    finally:
        cursor.close()
        conn.close()



# Example usage
if __name__ == "__main__":
   

    # Load configuration
    config_file = r'C:\Users\junzh\OneDrive\Desktop\snowflake_test\config\config.yaml'
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)['snowflake']

    query1 = "SELECT * FROM my_table"
    df = extract_data(query1, config)
    print(df)
    
    df1 = transform_data(df)
    load_data(df1,"my_table_transformed", config)
   



