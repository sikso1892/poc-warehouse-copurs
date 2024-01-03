#!/bin/bash

BUCKET="flitto"
FOLDER="s3-folder"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI not installed. Please install and configure it first."
    exit 1
fi

# List objects in the source folder and read into an array
objects_arr=()
while read -r line; do
    objects_arr+=("$line")
done < <(aws s3 ls "s3://$BUCKET/$FOLDER/" | awk '{print $4}')

# Print the objects in the array
echo "Objects in '$FOLDER':"
for object in "${objects_arr[@]}"; do
    aws s3 mv "s3://$BUCKET/s3-folder/$object" "s3://$BUCKET/$object"
    
    # Check if move was successful for each object
    if [ $? -eq 0 ]; then
        echo "Object '$object' successfully moved to '$DEST_FOLDER' in S3."
    else
        echo "Failed to move object '$object' to '$DEST_FOLDER' in S3."
    fi
done
