import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file: json files containing songfile data are opened
    
    df = pd.read_json(filepath, lines=True)
    
    # insert song record: dataframe for song_data is created 
    # song_data is filled with data from the song file using the sql command: song_table insert
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record: dataframe for artist_data is created
    # artist_data is filled with data from the songfile aswell using the sql command: artist_table_insert
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file: json files containing log_file data are opened
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action: All files with page containing next song are put into df
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime: ts is converted from ms into datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # time_data is filled with data from the log_file
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    
    # column labels are specified
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # Dataframe containing the column labels and time_data is created
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    for i, row in time_df.iterrows():
        
    # insert time data records: time data is insert into dataframe using the sql command: time_table_insert
        cur.execute(time_table_insert, list(row))
        #conn.commit()

    # load user table: Dataframe for user data is created
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records: user_table is filled with data using sql command user_table_insert
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records: creating a songplay dataframe
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables: 
        # get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
           song_id, artist_id = results
        else:
           song_id, artist_id = None, None

        # insert songplay record:
        # first convert timestamp to datetime again
        start_time = pd.to_datetime(row.ts, unit='ms')
        # filling songplay_table with data from the time_table, song_table and artist_table using the sql command songplay_table_insert
        songplay_data = (start_time,row.userId,row.level,song_id,artist_id,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        #conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # The following connects to the server
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # Here the cursor is created
    cur = conn.cursor()
    conn.autocommit = True
    
    # Here we run the function process_data to get the files for the song_file and log_file
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
