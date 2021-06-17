#!/usr/bin/env python2.7
import time

from epidata.EpidataLiteStreamingContext import EpidataLiteStreamingContext
import sys
import numpy
import pandas
import sqlite3
from sqlite3 import Error

import unittest

ec = EpidataLiteStreamingContext()

database = "/Users/lujiajun/Documents/epidata-intern/data/epidata_development.db"


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Error as e:
        print(e)

    return conn


con = create_connection(database)

def main():
    print("Hello World!")
    ec.printSomething('god')
    trans = ec.create_transformations("Identity", ["Meas-1"], {})
    trans2 = ec.create_transformations("Identity", ["Meas-1"], {})
    print(trans)
    print(trans2)
    ec.create_stream("measurements_original", "measurements_intermediate", trans)
    ec.create_stream("measurements_intermediate", "measurements_cleansed", trans)
    print("Start stream")
    ec.start_stream()
    print("Enter 'Q' to stop streaming")

    state = True
    while state:
        data = raw_input("enter q to quit")
        if data == 'q' or data == 'Q':
            state = False


    ec.stop_stream()

if __name__ == "__main__":
    main()