#!/bin/sh
#
#
# python3 update_stockdb.py > output.log 2>&1
python3 update_stockdb.py --dropdb --historydb --update_by_nikkei > output.log 2>&1

