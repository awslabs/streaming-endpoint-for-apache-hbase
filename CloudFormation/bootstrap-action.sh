#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
set -x

LIB_LOCAL_PATH=/usr/lib/hbase-extra
#sudo aws s3 cp $LIB_S3_PATH $LIB_LOCAL_PATH
JARFILENAMES="kafka-sink-alpha-0.1.jar,kinesis-sink-alpha-0.1.jar,sensor-simulator-alpha-0.1.jar"
REPOPATH="repos/awslabs/streaming-endpoint-for-apache-hbase"

sudo yum install jq

#get Asset ID
function get_assetid() {
	echo `curl -H "Accept: application/vnd.github.v3+json" https://api.github.com/$REPOPATH/releases | jq --arg name $1 '.[].assets[] | select(.name == $name).id'`
}

function download_asset() {
	ASSETID=`get_assetid $1` 
	OUTPUT=$1
	if [ -z "$2" ]; then
		OUTPUT="/tmp/$OUTPUT"
	else
		OUTPUT="$2/$1"
	fi
	sudo curl -L -o $OUTPUT -H "Accept: application/octet-stream" https://api.github.com/$REPOPATH/releases/assets/$ASSETID
}

sudo mkdir $LIB_LOCAL_PATH

for JARNAME in `echo $JARFILENAMES | awk 'BEGIN {RS=","} { print $1 }'`; do
	echo $JARNAME
	download_asset "$JARNAME" "$LIB_LOCAL_PATH" 
done
