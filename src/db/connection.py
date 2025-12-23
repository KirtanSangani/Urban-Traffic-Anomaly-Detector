import psycopg2
import os
import time

#Database Configuration
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'traffic_anomaly')
DB_USER = os.getenv('DB_USER', 'traffic_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'traffic_pass')

#Database Connection
def get_db_connection():
    #Retrying to connect to the database 3 times
    for i in range(3):
        try:
            #Connecting to the database
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print(f"Connected to the database: {DB_NAME}")
            break
        except psycopg2.Error as e:
            #If the connection fails, retry 2 more times
            if i < 2:
                print(f"Failed to connect to the database: {e}, retrying...")
                time.sleep(1)
            else:
                #If the connection fails, raise an error
                print(f"Failed to connect to the database: {e}")
                raise e
    
    #If the connection fails, return None
    if conn is None:
        return
    
    #Inserting data into the test table
    try:
        #Creating a test table
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS connection_test (
            id SERIAL PRIMARY KEY,
            test_value VARCHAR(50)
        );
        """)

        conn.commit()
        print("Test table created successfully.")

        #Inserting data into the test table
        test_data = f"Data written at {time.strftime('%H:%M:%S')}"
        cursor.execute("INSERT INTO connection_test (test_value) VALUES (%s);", (test_data,))
        conn.commit()
        print(f"Data inserted: '{test_data}'")

        cursor.execute("SELECT test_value FROM connection_test ORDER BY id DESC LIMIT 1;")
        result = cursor.fetchone()

        print(f"Data read successfully: '{result[0]}'")
    except Exception as e:
        #If the error occurs, rollback the transaction
        print(f"Error: {e}")
        conn.rollback()
    finally:
        #Closing the connection
        if conn:
            conn.close()

#Main function
if __name__ == "__main__":
    get_db_connection()