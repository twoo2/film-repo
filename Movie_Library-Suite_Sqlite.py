import os
import sqlite3
import requests
import sys
import time
import gzip
import shutil
import csv
from sqlite3 import Error
from datetime import datetime 
from colorama import Fore, Back, Style # Reference: https://www.geeksforgeeks.org/print-colors-python-terminal/

root = "/media/pi/U_wanna_fite/Movies/"
database = r"/home/pi/Desktop/Scripts/db/Videos_DB.db"
imdatabase = r"/home/pi/Desktop/Scripts/db/IMDb.db"
checkroot = os.path.isdir(root) # Check if HDD is connected by validating directory

### DB Files ###
filesd = {'/NameBasics/':'name.basics.tsv.gz','/TitleAkas/':'title.akas.tsv.gz','/TitleBasics/':'title.basics.tsv.gz','/TitleCrew/':'title.crew.tsv.gz'}
linkpath = "https://datasets.imdbws.com/"
filepath = "/home/pi/Desktop/Scripts/IMDb"

masterlist = list()

if checkroot:
    print("External HDD Connected... please select an option")
    print("""
    1. Display all titles to Terminal
    2. Update/Generate DB File
    3. Download new IMDb export
    4. Update/Generate IMDb File
    NULL. Quit
    """)
    print("Enter Option: ")
    option = input()
    os.chdir(root)
    subroot = os.listdir(root) # Creates list of all subroot directories
    subroot = [root + x + '/' for x in subroot if not x.endswith('.txt') and not x.startswith('_')] # Append full root path to subroot directory name and remove unwanted directories
    subroot.sort()

    class Movie:
        def __init__(self,parentdir,typeof,name,director,daterelease,dateadded,runtime):
            self.parentdir = parentdir
            self.typeof = typeof
            self.name = name
            self.director = director
            self.daterelease = daterelease
            self.dateadded = dateadded
            self.runtime = runtime
    
    class TV:
        def __init__(self,parentdir,typeof,name,episode,season,daterelease,dateadded,runtime):
            self.parentdir = parentdir
            self.typeof = typeof
            self.name = name
            self.episode = episode
            self.season = season
            self.daterelease = daterelease
            self.dateadded = dateadded
            self.runtime = runtime

    def subfolders(path_to_parent):
        moviefiles = os.listdir(path_to_parent)
        for x in moviefiles:
            if os.path.isdir(path_to_parent + "/" + x):
                return True
            else:
                return False
            
    def getMovies(currentdir): # Function to print out all the movie titles and their respective modified date and time
        print(Fore.GREEN + "CHECKING: " + currentdir + Style.RESET_ALL)
        print('%-16s %-55s %5s %18s' % ("DIR:", "TITLE:", "TYPE:", "LAST MODIFIED:")) # SHOULD PRINT BE HERE OR OUTSIDE FUNCTION DEF?
        movielist = list()
        tvlist = list()
        for i in os.listdir(currentdir):
            videotype = ""
            videotitle = i
            director = "#N/A" # PLACEHOLDER
            episode = 0 # PLACEHOLDER
            season = 0 # PLACEHOLDER
            daterelease = "#N/A" # PLACEHOLDER
            modifieddate = str(datetime.fromtimestamp(os.path.getmtime(currentdir + i))) # Gets timestamp of directory and converts to datetime object
            runtime = 0 # PLACEHOLDER
            if subfolders(currentdir + i):
                videotype = "TV"
                title = TV(currentdir, videotype, videotitle, episode, season, daterelease, modifieddate, runtime) # Input variables into class and set to title
                tvlist.append(title) # Add class variable to list
            else:
                videotype = "Movie"
                title = Movie(currentdir, videotype, videotitle, director, daterelease, modifieddate, runtime) # Input variables into class and set to title
                movielist.append(title) # Add class variable to list
            print('%-16s %-55s %5s %30s' % (title.parentdir[-4:-1], title.name, title.typeof, title.dateadded)) # SHOULD PRINT BE HERE OR OUTSIDE FUNCTION DEF?
        return tvlist, movielist
    
    def create_connection(db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)
        return conn

    def create_table(conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)

    def create_video(conn, title):
        """
        Create a new title into the movies table
        :param conn:
        :param title:
        :return: title id
        """
        if title[1] == "Movie":
            sql = '''INSERT OR IGNORE INTO Movies(Directory,Type,Title,Director,ReleaseDate,LastModified,Runtime) VALUES(?, ?, ?, ?, ?, ?, ?)'''
        else:
            sql = '''INSERT OR IGNORE INTO TV_Shows(Directory,Type,Title,Episode,Season,ReleaseDate,LastModified,Runtime) VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

        cur = conn.cursor()
        cur.execute(sql, title)
        return cur.lastrowid
        
    if option == "2":
        sql_create_movies_table = """ CREATE TABLE IF NOT EXISTS Movies (
                                            Directory varchar(255),
                                            Type varchar(255),
                                            Title varchar(255) UNIQUE,
                                            Director varchar(255),
                                            ReleaseDate varchar(255),
                                            LastModified varchar(255),
                                            Runtime int
                                        ); """

        sql_create_tv_table = """ CREATE TABLE IF NOT EXISTS TV_Shows (
                                            Directory varchar(255),
                                            Type varchar(255),
                                            Title varchar(255) UNIQUE,
                                            Episode int,
                                            Season int,
                                            ReleaseDate varchar(255),
                                            LastModified varchar(255),
                                            Runtime int
                                        ); """
        # create a database connection
        conn = create_connection(database)
        
        with conn:
            # create tables
            if conn is not None:
                # create videos table
                create_table(conn, sql_create_movies_table)
                create_table(conn, sql_create_tv_table)
                print("Database table not found--generating file")
            else:
                print("Error! cannot create the database connection.")

            for x in subroot:
                tvlist, movielist = getMovies(x)
                masterlist.append(tvlist)
                masterlist.append(movielist) # masterlist is currently unused, maybe store as dictionary object for cross checking removed movies?
                for y in tvlist:
                    entry = (z.parentdir[-4:-1], y.typeof, y.name, y.episode, y.season, y.daterelease, y.dateadded, y.runtime)
                    create_video(conn, entry)
                for z in movielist:
                    entry = (z.parentdir[-4:-1], z.typeof, z.name, z.director, z.daterelease, z.dateadded, z.runtime)
                    create_video(conn, entry) 
                cur = conn.cursor()
                cur.execute('''SELECT * from Movies, TV_Shows ORDER BY Title DESC''')
            
    if option == "3":
        def downloadDB(dllink,path):
            with open(path, "wb") as f:
                print("Downloading %s" % path)
                response = requests.get(dllink, stream=True)
                total_length = response.headers.get('content-length')
                if total_length is None: # no content length header
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in response.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                        sys.stdout.flush()
                    print("Download complete %s" % path)
        
        def extractDB(sourcepath,destpath): # Need to implement a status bar
            with gzip.open(sourcepath, 'rb') as f_in:
                print('Extracting file %s' % sourcepath)
                with open(destpath+'/data.tsv', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("DONE...")

        print("Download files? (Y/N)")
        confirm = input()

        if confirm == "Y" or confirm == "y":
            for key, value in filesd.items():
                downloadDB(linkpath+value,filepath+key+value)

        print("Extract files? (Y/N)")
        confirm = input()

        if confirm == "Y" or confirm == "y":
            for key, value in filesd.items():
                extractDB(filepath+key+value,filepath+key)
    
    if option == "4":
        for key, value in filesd.items():
            print("Checking for existing data.tsv file in "+filepath+key)
            if os.path.isfile(filepath+key+"data.tsv"):
                print("All good")
            else:
                print("Coud not find valid data.tsv file")
                if os.path.isfile(filepath+key+value):
                    print("Export found... Extract files? (Y/N)")
                    confirm = input()
                    if confirm == "Y" or confirm == "y":
                        extractDB(filepath+key+value,filepath+key)
                else:
                    print("Could not find valid export, please re-download from menu")

        sql_create_imdb_table = """ CREATE TABLE IF NOT EXISTS IMDb (
                                            TitleKey varchar(255),
                                            Title varchar(255) UNIQUE,
                                            ReleaseDate varchar(255),
                                            Genres varchar(255),
                                            Runtime varchar(255)
                                        ); """
        # Placeholder #
        with open("/home/pi/Desktop/Scripts/IMDb/TitleBasics/data.tsv") as tsvfile:
            reader = csv.DictReader(tsvfile, dialect='excel-tab')
            conn = create_connection(imdatabase)
            if conn is not None:
                # create videos table
                create_table(conn, sql_create_imdb_table)
                print("Database table not found--generating file")
            else:
                print("Error! cannot create the database connection.")

            i = 0
            with conn:
                for row in reader:
                    if (row['titleType'] == "movie" or row['titleType'] == "tvseries" or row['titleType'] == "tvepisode") and row['isAdult'] == '0' and row['runtimeMinutes'] != "\\N" and row['startYear'] != "\\N": # 
                        sql = '''INSERT OR IGNORE INTO IMDb(TitleKey,Title,ReleaseDate,Genres,Runtime) VALUES(?, ?, ?, ?, ?)'''
                        title = (row['tconst'],row['primaryTitle'],row['startYear'],row['genres'],row['runtimeMinutes'])
                        print(i)
                        cur = conn.cursor()
                        cur.execute(sql, title)
                        i += 1


else:
    print("External HDD not found... please connect and try again")
    exit

''' 
TO DO:
- DONE: Refactor subfolder check
- DONE: Create class per subroot
- DONE: Export Sqlite database
- DONE: Check for new entries and add
- DONE: Check for new entries and alphabetically sort
- Remove missing entries from database
- Export report mechanism
- Status bar for extracting files
- Set all class objects as elements of the movie
'''

### NOTES ###
# Default Python install location: C:\Users\Wooster\AppData\Local\Programs\Python\Python38-32
# Installing SQL: https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15
# SQL Server Management Studio (SSMS): https://docs.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver15
# SQL Server: WOOSTER-PC\SQLEXPRESS
# SQL Instance: SQLEXPRESS
# SQL User Name: WOOSTER-PC\Wooster
# SQL Authentication: Windows Authentication
# Connecting to SQL via PYODBC: https://datatofish.com/how-to-connect-python-to-sql-server-using-pyodbc/
# Python SQL Query: https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver15
# Add checking mechanism
# Add sorting mechanism
# Ensuring the correct interpreter environment is selected in VS Code: https://code.visualstudio.com/docs/python/environments
# Python progress bar: https://stackoverflow.com/questions/15644964/python-progress-bar-and-downloads/15645088
# IMDb Datasets: https://datasets.imdbws.com/
# IMDb Interfaces: https://www.imdb.com/interfaces/

