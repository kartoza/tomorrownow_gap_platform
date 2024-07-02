#!/bin/bash

file="/etc/bes/credentials.conf"

if [ -f "$file" ] ; then
    rm "$file"
fi

touch "$file"
echo "tomorrownow = url:${S3_AWS_ENDPOINT}${S3_AWS_BUCKET_NAME}/" >> "$file"
echo "tomorrownow += id:${S3_AWS_ACCESS_KEY_ID}" >> "$file"
echo "tomorrownow += key:${S3_AWS_SECRET_ACCESS_KEY}" >> "$file"
echo "tomorrownow += region:us-east-1" >> "$file"
echo "tomorrownow += bucket:${S3_AWS_ENDPOINT}" >> "$file"

chmod 600 "$file"
chown bes:bes "$file"

# run original entrypoint
./entrypoint.sh