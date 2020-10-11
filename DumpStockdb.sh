#!/bin/sh
#
#
mysqldump -u stockdb -p -h 192.168.1.11 stockdb data_j stockdb --single-transaction > stockdb.dump
zip stockdb.zip stockdb.dump
rm stockdb.dump
