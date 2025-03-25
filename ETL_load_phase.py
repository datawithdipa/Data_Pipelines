import psycopg2
import os

def delete_old_records(conn, csv_file_path):
    cur = conn.cursor()

    # Execute the delete query
    delete_query = "DELETE FROM earthquake WHERE filename=%s"
    cur.execute(delete_query, (csv_file_path,))
    conn.commit()
    cur.close()


def load_csv_to_postgres(conn, csv_file_path):
    cur = conn.cursor()
    sql = "COPY earthquake FROM STDIN DELIMITER ',' CSV HEADER"
    cur.copy_expert(sql, open(csv_file_path, "r"))
    conn.commit()
    cur.close()


def main():
    # Database connection parameters
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASS'),
        'host': os.getenv('DB_HOST'),
        'port': ''
    }

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)

    # Path to your CSV file
    csv_file_path = '/Users/dipukuttan/Documents/pipeline/data/earthquake_2025_03_25.csv'

    try:
        # Delete old records
        delete_old_records(conn, csv_file_path)

        # Load CSV data into the table
        load_csv_to_postgres(conn, csv_file_path)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        conn.close()


if __name__ == "__main__":
    main()
