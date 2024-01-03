#!/bin/bash

# Script to upload a CSV file to AWS S3

# Variables
BUCKET="flitto"      
SOURCE_DIR=data/Normalization/2023-12-26         

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null
then
    echo "AWS CLI not installed. Please install and configure it first."
    exit 1
fi

for file in "$SOURCE_DIR"/*csv; do
    # Upload file to S3
    aws s3 cp "$file" "s3://$BUCKET"
    
    # Check if upload was successful for each file
    if [ $? -eq 0 ]; then
        echo "File '$file' successfully uploaded to S3."
    else
        echo "File '$file' upload failed."
    fi
done

# Check if upload was successful
if [ $? -eq 0 ]; then
    echo "File successfully uploaded to S3."
else
    echo "File upload failed."
fi
