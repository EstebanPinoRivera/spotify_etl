import psycopg2
from sqlalchemy import create_engine
import pandas as pd

# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'spotify_db',
    'user': 'postgres',
    'password': 'password',
    'port': '5432',
}

# Function to create tables in the database
def create_tables():
    # Define CREATE TABLE queries
    create_table_query1 = '''
    CREATE TABLE IF NOT EXISTS album(
    album_id VARCHAR(100),
    album_name VARCHAR(100),
    release_date VARCHAR(100),
    album_total_tracks INT,
    album_url VARCHAR(100)
    );
    '''

    create_table_query2 = '''
    CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR(100),
    artist_names VARCHAR(100),
    external_url VARCHAR(100)
    );
    '''

    create_table_query3 = '''
    CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR(100),
    song_name VARCHAR(100),
    song_duration INT,
    song_url VARCHAR(100),
    song_popularity INT,
    song_added VARCHAR(100),
    album_id VARCHAR(100),
    artist_id VARCHAR(100)
    );
    '''

    try:
        # Establish the connection
        connection = psycopg2.connect(**db_params)

        # Create a cursor
        with connection.cursor() as cursor:
            # Execute CREATE TABLE queries
            cursor.execute(create_table_query1)
            cursor.execute(create_table_query2)
            cursor.execute(create_table_query3)

        # Commit changes to the database
        connection.commit()

    except Exception as e:
        print("Error in connection or table creation:", e)

    finally:
        # Close the connection
        if connection:
            connection.close()

# Function to insert data into a table in the database
def insert_data_to_table(df, table_name):
    # Database engine parameters
    engine_params = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(engine_params)

    try:
        # Insert data into the table
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f'Data inserted into the {table_name} table')
    except Exception as e:
        print(f'Error inserting data into the {table_name} table: {e}')

# Main block: Execute table creation and data insertion when the script is run
if __name__ == "__main__":
    # Data from DataFrames
    df_album = pd.read_csv('data/album.csv')
    df_artist = pd.read_csv('data/artist.csv')
    df_song = pd.read_csv('data/song.csv')

    # Create tables in the database
    create_tables()

    # Insert data into the tables
    insert_data_to_table(df_album, 'album')
    insert_data_to_table(df_artist, 'artist')
    insert_data_to_table(df_song, 'song')
