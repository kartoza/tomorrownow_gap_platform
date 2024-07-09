#!/bin/bash

file="/etc/bes/credentials.conf"

if [ -f "$file" ] ; then
    rm "$file"
fi

touch "$file"
echo "tomorrownow = url:${AWS_ENDPOINT_URL}${S3_AWS_BUCKET_NAME}/" >> "$file"
echo "tomorrownow += id:${AWS_ACCESS_KEY_ID}" >> "$file"
echo "tomorrownow += key:${AWS_SECRET_ACCESS_KEY}" >> "$file"
echo "tomorrownow += region:us-east-1" >> "$file"
echo "tomorrownow += bucket:${AWS_ENDPOINT_URL}" >> "$file"

chmod 600 "$file"
chown bes:bes "$file"

# run original entrypoint
./entrypoint.sh