#!/bin/bash

file="/etc/bes/credentials.conf"

if [ -f "$file" ] ; then
    rm "$file"
fi

touch "$file"
echo "tomorrownow = url:http://minio:9000/tomorrownow/" >> "$file"
echo "tomorrownow += id:minio_tomorrownow" >> "$file"
echo "tomorrownow += key:minio_tomorrownow" >> "$file"
echo "tomorrownow += region:us-east-1" >> "$file"
echo "tomorrownow += bucket:tomorrownow" >> "$file"

chmod 600 "$file"
chown bes:bes "$file"

# run original entrypoint
./entrypoint.sh