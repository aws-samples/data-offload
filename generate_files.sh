#!/bin/bash

# Sample code, software libraries, command line tools, proofs of concept, templates, or other related technology are provided as AWS Content or Third-Party 
# Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content or 
# Third-Party Content in your production accounts, or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS 
# Content or Third-Party Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. 
# Deploying AWS Content or Third-Party Content may incur AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or 
# using Amazon S3 storage.”

# Check if two parameters are given
if [ "$#" -ne 2 ]; then
    echo "Usage: ./generate_files.sh <File size in GB> <Number of files>"
    exit 1
fi
# Convert file size from gigabytes to bytes (1 GB = 1073741824 bytes)
file_size_bytes=$(($1 * 1073741824))
# Create the specified number of files
for ((i=1; i<=$2; i++)); do
    dd if=/dev/urandom of=random_file_$i bs=1024 count=$(($file_size_bytes / 1024))
done
