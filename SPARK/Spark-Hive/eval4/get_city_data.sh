#!/bin/bash


# Hive script to get data for a specific city

hive -hiveconf cityName="$1" -f city_data.hive

