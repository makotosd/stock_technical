#!/bin/sh
#
#
# mysqldump -u stockdb -p -h 192.168.1.11 stockdb data_j stockdb --single-transaction > stockdb.dump
mysqldump --single-transaction -u stockdb -p stockdb --password=xxxxxxx > stockdb.dump
zip /mnt/e/stockdb.zip stockdb.dump
rm stockdb.dump
