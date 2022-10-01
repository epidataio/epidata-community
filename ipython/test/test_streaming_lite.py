#!/usr/bin/env python2.7

from datetime import datetime, timedelta

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


class Base(unittest.TestCase):

    def assertEqualRows(self, one, two):
        self.maxDiff = None
        # if not isinstance(one, Column):
        self.assertEqual(one, two)

    def assertEqualDataFrames(self, one, two):
        self.assertEqual(one.count(), two.count())
        for i, j in zip(one, two):
            self.assertEqualRows(i, j)


class EpidataContextTests(Base):
    # def test_simple_query_test(self):
    #     # create a database connection
    #     cur = con.cursor()
    #     cur.execute("select * from measurements_original where key2 = 'Test-1'")
    #     rows = cur.fetchall()
    #
    #     for row in rows:
    #         print(row)

    def test_simpleTest(self):

        ec.printSomething('god')
        trans = ec.createTransformations("Identity", ["Meas-1"], {})
        print(trans)
        ec.createStream("measurements_original", "measurements_cleansed", trans)
        print("Start stream")
        ec.startStream()
        print("Enter 'Q' to stop streaming")
        # data = input("Enter a number to quit")
        # while data:
        #     data = input("Enter a number to quit")
        ec.stopStream()


if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
