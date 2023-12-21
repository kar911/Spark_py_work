#!/bin/bash

# Author: Rishabh
# Creation Date: 21/11/2023
# Last Modified: 21/11/2023
# Description
# This script will create a table 'dy_hiredata' in hive and load data

# Exit if any command has non zero status 
set -e

# Create a table 'dy_hiredata'
hive -f create_table.hive

# Load the data into the table 'dy_hiredata'
hive -f load_data.hive
