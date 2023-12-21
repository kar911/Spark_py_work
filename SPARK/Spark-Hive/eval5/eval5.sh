#!/bin/bash

#Author: Rishabh
# Creation Date: 21/11/2023
# Modified Date: 21/11/2023
# Description:
# This script will execute 3 hive file to create a table 'startups' and to laod a data and then to remove header from the table

# Exit if any command has a non zero status
set -e

# Create a table 'startups in Hive'
hive -f create_table.hive

# Load data into the table 'startups'
hive -f load_data.hive

# Remove header from the table 'startups'
hive -f remove_header.hive


 
