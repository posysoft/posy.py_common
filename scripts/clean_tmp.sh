#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 folder filter ttl"
    exit 1
fi

FOLDER=$1
FILTER=$2
TTL=$3
if [ -z "$TTL" ]; then
    TTL=$[24*3]
fi
TTL=$[`date +%s` - 3600*$TTL]

echo '###########################################################'
echo  remove files in \"$FOLDER\" modified before \"`date -d @$TTL "+%F %T"`\" filtered by \"$FILTER\"

for f in `ls "$FOLDER" | grep -E "$FILTER"`; do
    file="$FOLDER/$f"
    t=`stat -c %Y "$file"`
    if [ $t -lt $TTL ]; then
        echo remove: "$file" : `date -d @$t "+%F %T"`
        rm -rf "$file"
    fi
done
#echo \"$FOLDER\" \"$FILTER\" \"$TTL\"
