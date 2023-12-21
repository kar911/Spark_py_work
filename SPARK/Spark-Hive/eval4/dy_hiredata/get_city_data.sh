#!/bin/bash


# Hive script to get data for a specific city
# Exit if the command has non zero status
set -e

hive -hiveconf cityName="$1" -f city_data.hive

