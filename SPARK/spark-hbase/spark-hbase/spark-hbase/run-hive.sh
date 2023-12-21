#!/bin/bash
# setup script for running hive script through beeline
# prerequisit is hive metastore(run-hivemetastore.sh) and hive-server2(run-hiveserver2.sh) services are running

declare dbname=default
declare DDLPath=/home/talentum/spark-jupyter/spark-hbase/spark-hbase/spark-hbase/

echo "-----Creating Hive table on top of hbase----- "
beeline -u jdbc:hive2://localhost:10000/$dbname -n hiveuser -p Hive@123 --hivevar dbname=$dbname -f $DDLPath/hbase-hive.hive
#beeline -u jdbc:hive2://localhost:10000/$dbname -n hiveuser -p Hive@123 -e "select * from stocks_hbase limit 10;"
