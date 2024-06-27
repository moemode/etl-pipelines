#!/bin/bash

# Define variables
URL="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"
GZ_FILE="web-server-access-log.txt.gz"
TXT_FILE="web-server-access-log.txt"
CSV_FILE="transformed-data.csv"
DB_NAME="template1"
DB_USER="postgres"
DB_TABLE="access_log"

# Download the file
echo "Downloading the file..."
curl -O $URL

# Extract the .txt file using gunzip
echo "Extracting the file..."
gunzip -f $GZ_FILE

# Extract timestamp, latitude, longitude and visitorid 
# which are the first four fields from the file using the cut command.
cut -d'#' -f1-4 $TXT_FILE > extracted-data.txt
# Transform the text delimiter from "#" to ","
tr "#" "," < extracted-data.txt > $CSV_FILE

# Load phase
echo "Loading data"
psql -h localhost -U $DB_USER -d $DB_NAME -c "\COPY $DB_TABLE FROM '$CSV_FILE' WITH (FORMAT csv, HEADER true)"

# echo "ETL process completed successfully."
