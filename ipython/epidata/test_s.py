#!/usr/bin/env python2.7
#
# Copyright (c) 2015-2022 EpiData, Inc.
#
import time

from epidata.EpidataLiteStreamingContext import EpidataLiteStreamingContext
import sys
import numpy
import pandas
import sqlite3
from sqlite3 import Error

import unittest


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Error as e:
        print(e)

    return conn


con = create_connection(db_url)

def main():
    ec = EpidataLiteContext()

    print("EpiData Lite - Batch Test Started")

#    config_file = open("../resources/sqlite_defaults.conf")
    db_url = "../../data/data/epidata_development.db"

    ### To Be Completed
    print("EpiData Lite - Batch Test")



    elc = EpidataLiteStreamingContext()
    ec.printSomething('something')
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
