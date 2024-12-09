import dotenv
import duckdb
import os
dotenv.load_dotenv()

def test_connection():
    conn = duckdb.connect(database=os.getenv('DUCKDB_PATH'))
    cursor = conn.cursor()
    #check data type of payment table
    cursor.execute("SELECT * FROM payment")
    result = cursor.fetchall()
    print(result)
    assert len(result) > 0, "No data returned from the query"
    conn.close()

if __name__ == "__main__":
    test_connection()