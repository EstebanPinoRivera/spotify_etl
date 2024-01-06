import psycopg2

db_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'password',
    'port': '5432'
}

try:
    connection = psycopg2.connect(**db_params)

    cursor = connection.cursor()
    
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("Version PostgreSQL:", version)

except Exception as e:
    print("Error:", e)

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
