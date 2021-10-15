#!/usr/bin/env python3  
# -*- coding: utf-8 -*

#---------------------------------------------------------------------------------
# Duration to work (dur2work.py)
# 
# A python script that collects the travel duration of a route request from the
# googlemaps duration API and writes the result in a SQLite database.
#
# SQLite table format:
#+-------------------------------------------+ track_id:    unique integer id of the track
#|                   route                   | start:       start addres of the route
#+----------+-------+-------------+----------+ destination: destination address 
#| track_id | start | destination | duration | duration:    google duration result 
#+----------+-------+-------------+----------+              (without traffic consideration)
#|          |       |             |          |
#+----------+-------+-------------+----------+
#
#+---------------------------------------+ track_id: track id from route table
#|             track_duration            | duration_in_traffic: Google duration result
#+----------+------+---------------------+                      (considering traffic)
#| track_id | time | duration_in_traffic | time: timestamp of the request in seconds
#+----------+------+---------------------+       since since midnight, January 1, 1970 UTC
#|          |      |                     |
#+----------+------+---------------------+
#
#
# The start and destination of the route are passed as arguments to the script.
# Use i.e. the home address as start and the work address as destination for the
# way to work. For the way home reverse the start and destination address.
#
# The script is intended to be used as a cronjob target on a linux server.
# I. e. ehe script may  be called every 5 minutes from 5-9am and 14-19pm for a view
# months. By processing the data collected in the SQLite database the optimum 
# starting time for the way to work and back home can be determined
#
# Excample crontab entries (crontab -l)
#   */5 5-9 * * 1-5 python3 /home/holzi/dur2work/dur2work.py "<HOME_ADDRESSS>" "<WORK_ADDRESSS>"
#   */5 14-19 * * 1-5 python3 /home/holzi/dur2work/dur2work.py "<WORK_ADDRESSS>" "<HOME_ADDRESSS>"
#
#---------------------------------------------------------------------------------
# Prerequisites
# - googlemaps python module (pip install googlemaps)
# - Googlemaps directions API key located in a textfile: api_key.txt in the same
#   folder as the script
#---------------------------------------------------------------------------------
# usage: dur2work.py [-h] start destination
# Writes resonses from google maps drections API to a SQLite database
# positional arguments:
#   start        Start/beginning of the track
#   destination  Destination/end of the track
#---------------------------------------------------------------------------------
#
# Created By  : Tobias Ammann
# Created Date: 14.10.2021
# version ='1.0'
# ---------------------------------------------------------------------------

import os                                 # Get the path of the script
import sys                                # Get path of the script

from datetime import timezone, datetime   # Working with time objects
import sqlite3                            # SQLite database
import argparse                           # Command line arguments
import logging                            # Logging

import googlemaps                         # Google maps API

# Logfile: Write to console and logfile
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(sys.path[0], "dur2work.log")),
        logging.StreamHandler()
    ]
)

# Argparse
parser = argparse.ArgumentParser(description='Writes resonses from google maps drections API ' \
                                             'to a SQLite database')
# Required arguments
parser.add_argument('start',   help = 'Start/beginning of the track')
parser.add_argument('destination', help = 'Destination/end of the track') 

# Parse
args = parser.parse_args()
start = args.start
destination = args.destination

# Read the API key for the googlemaps API from the keyfile
try:
    api_key_file = open(os.path.join(sys.path[0], "api_key.txt"), "r")
    API_KEY = api_key_file.read()
    api_key_file.close()
except Exception as e:
    logging.error('Error reading API key from "api_key.txt"')
    logging.error(e)
    exit()

# Create a googemaps client object
gmaps = googlemaps.Client(key=API_KEY)

# Get the current time in seconds since POSIX, independent of the system epoch
epoch = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)     #POSIX epoch                              
curr_time = datetime.now(timezone.utc)                      #Current UTC time 
curr_time = curr_time - epoch                               #calculate the time difference to a user defined epoch
curr_time = curr_time.total_seconds()                       #get number of seconds since epoch
#print(curr_time - datetime.now(timezone.utc).timestamp() )

# Request directions via public transit at current departure time
try:
    res = gmaps.directions( start,
                            destination,
                            mode='driving',
                            units='metric',
                            traffic_model='best_guess',
                            departure_time=int(curr_time))       # Directions API Definition: Integer in seconds 
                                                            # since midnight, January 1, 1970 UTC
except Exception as e:
    logging.error(e)
    exit()

if len(res) <= 0:
    # Googlemaps API route was found for the requested start/end adrees -> end programm
    logging.error('Google maps API did not return a route for the requested ' \
                    'start-/end-address.')
    exit()

# The result is a list containing a dict -> extract legs
legs = res[0]['legs'][0]
duration_in_traffic = legs['duration_in_traffic']['value']
duration            = legs['duration']['value']

# Database handling
try:
    db_con = sqlite3.Connection(os.path.join(sys.path[0], "dur.db"))
    db_cur = db_con.cursor()

except Exception as e:
    logging.error(e)
    exit()
    
# Create route database
db_cur.execute('''CREATE TABLE IF NOT EXISTS route
                     (track_id INTEGER PRIMARY KEY,
                     start TEXT , 
                     destination TEXT ,
                     duration REAL)''')

# Create track_duration database
db_cur.execute('''CREATE TABLE IF NOT EXISTS track_duration
                     (track_id INTEGER,
                     time REAL , 
                     duration_in_traffic REAL)''')

# Query start and destination
db_cur.execute('''SELECT track_id FROM route WHERE
                            start = "{}"  COLLATE NOCASE AND
                            destination = "{}"
                            COLLATE NOCASE'''.format(start, destination))
ids_known_tracks = db_cur.fetchall() 

# Check if the start and end destination are already known (with an assigned track id)
# Otherwise create a new id 
if len(ids_known_tracks) == 0:

    # Start and end address unkown, add a new database table
    logging.info('Unkown route, adding new entry in "route" database')

    # Fetch current maximum id
    db_cur.execute('''SELECT MAX(track_id) from route''')
    max_id = db_cur.fetchall() 

    # Initialize max_id if the database was empty
    if max_id[0][0] is None:
        max_id[0] = (-1,)

    # Increment the id
    track_id = max_id[0][0] + 1 

    # Insert new data into the route table
    db_cur.execute('''INSERT INTO route 
                        VALUES ({}, '{}', '{}', {})'''.format(track_id, start, destination, duration))
    db_con.commit()

elif len(ids_known_tracks) == 1:

    # Start and end address known, get track id
    track_id = ids_known_tracks[0][0]
else:
    logging.error("Database error: A SELECT MAX query returned more than one result")
    exit()

# Insert new data into the track_duration table
db_cur.execute('''INSERT INTO track_duration 
                    VALUES ({}, '{}', {})'''.format(track_id, 
                                                    curr_time,
                                                    duration_in_traffic))
db_con.commit()

#Log result
logging.info('start = ({}) - destination = ({}) - duration_in_traffic = {:.2f}min'.format(start, destination, duration_in_traffic/60))

# # Print entire database
# print("Database: route")
# for row in db_cur.execute('''SELECT * FROM route'''):
#    print(row)

# # Print entire database
# print("\nDatabase: track_duration")
# for row in db_cur.execute('''SELECT * FROM track_duration'''):
#    print(row)