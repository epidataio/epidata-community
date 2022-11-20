#!/bin/sh
#
# Copyright (c) 2015-2022 EpiData, Inc.
#

ps -ef | grep epidata-spark-assembly | grep -v grep | awk '{print $2}' | xargs kill
