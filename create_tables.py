import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database: The following lines connect the program to the server and creates a cursor
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding: First we drop all the preexisting databases
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    
    # now we create the new database using the sql code from sql_queries
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database: here we close the connection to the database
    conn.close()    
    
    # connect to sparkify database: Here we connect to the sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
   # Drops each table using the queries in `drop_table_queries` form the sql_queries list.
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    # We create each table using the queries in `create_table_queries` form the sql_queries list. 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Drops (if exists) and Creates the sparkify database using the above writen functions   
    # Establishes connection with the sparkify database and gets cursor to it.  
    cur, conn = create_database()
    
    # First all the tables are droped then the new tables are created
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # Finally the connection is closed
    conn.close()

# runs code only when the file is executed as a script
if __name__ == "__main__":
    main()
