#!/bin/bash

source functions.sh

WAIT=1


if [ "$#" == "1" ]
then
    IDS=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | sort | head -n $1`
else
    IDS=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | sort`
fi

echo $IDS


ALL=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | wc -l | tr -s " "`

CPT=0
for CAB_ID in $IDS
do
    CPT=$(($CPT + 1))
    echo -e "###\n# $CAB_ID [$CPT/$ALL]\n###"
    push_composite $CAB_ID
    sleep $WAIT
done

