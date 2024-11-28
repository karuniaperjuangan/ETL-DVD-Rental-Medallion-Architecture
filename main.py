import psycopg2
import duckdb
import dotenv
import os

dotenv.load_dotenv()

db_config ={
    "database":os.getenv("DB_NAME"),
    "user":os.getenv("DB_USER"),
    "password":os.getenv("DB_PASSWORD"),
    "host":os.getenv("DB_HOST"),
    "port":os.getenv("DB_PORT")
}

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./output/elt_dvd_rental.db")
os.makedirs("./output", exist_ok=True)

def connect_to_postgres():
    conn: psycopg2.extensions.connection = psycopg2.connect(**db_config)
    return conn

def connect_to_duckdb():
    conn = duckdb.connect(DUCKDB_PATH)
    return conn

if __name__== "__main__":
    postgres_conn = connect_to_postgres()
    duckdb_conn = connect_to_duckdb()

    # Load all tables from postgres, then insert into duckdb

    #Get List of Tables
    schema = 'dwh_schema'
    cursor = postgres_conn.cursor()
    cursor.execute(f"""SELECT table_name 
                   FROM information_schema.tables 
                   WHERE table_schema='{schema}' AND table_type='BASE TABLE'""")
    tables = [row[0] for row in cursor.fetchall()]

    #Load data form Postgres table and insert into duckdb
    for table in tables:
        print("Loading data from postgres table: ",table)
        query = f"SELECT * FROM {table}"
        cursor = postgres_conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # save data to csv first
        csv_path = f"./output/{table}.csv"
        with open(csv_path, "w") as file:
            file.write(",".join(columns) + "\n")
            for row in result:
                file.write(",".join(str(item) for item in row) + "\n")

        # Load data from csv into duckdb
        print("Loading data into duckdb table: ",table)
        duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_csv_auto('{csv_path}')")
        os.remove(csv_path)


        




    print("Data migration completed successfully.")


    # Close connections
    cursor.close()
    postgres_conn.close()
    duckdb_conn.close()