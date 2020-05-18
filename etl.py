import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # Load JSON file from songs' path
    df = pd.read_json(filepath,lines=True)

    # Get data from dataframe, convert to list and execute INSERT query for 'song'
    song_data = df[["song_id","title","artist_id","year","duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # Get data from dataframe, convert to list and execute INSERT query for 'artist'
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # Load JSON file from logs' path
    df = pd.read_json(filepath,lines=True).dropna()

    # Get only records with NextSong action
    df = df.where(df["page"] == "NextSong")

    # Get the timestamp of each record and create new colums
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms')
    df['hour'] = df['timestamp'].dt.hour
    df['day'] = df['timestamp'].dt.day
    df['week_year'] = df['timestamp'].dt.weekofyear
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    df['weekday'] = df['timestamp'].dt.weekday
    df['start_time'] = df['timestamp'].astype(str)

    # Get data from dataframe, convert to list and execute INSERT query for 'time'
    time_df = df[["start_time","hour","day","week_year","month","year","weekday"]].dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Get data from dataframe, convert to list and execute INSERT query for 'user'
    user_df = df[["userId","firstName","lastName","gender","level"]]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # Get data from dataframe, convert to list and execute INSERT query for 'songplays'
    # Some values have to be retrieved executing a JOIN query
    for index, row in df.iterrows():
        
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Creat the INSERT for the songplay record
        songplay_data = [row.start_time, row.userId, row.level, songid,artistid, row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # Get all files in a directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # DEBUG: number of files
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    # Connect to the database and get the cursor
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Process the files in song path
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    # Process the files in logs path
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    # Execute main method
    main()