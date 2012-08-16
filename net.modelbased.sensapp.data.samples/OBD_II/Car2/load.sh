#!/bin/bash
# author: Sebastien Mosser

## Loading the SensApp bash API
source ../../api.sh

for descr in `ls descriptors/*.json`
do
    name=`cat $descr | grep id | cut -d ":" -f 2 | cut -d \" -f 2`
    register_sensor $name $descr
done

for file in `ls json/*.json`
do
    db_raw_import $file
done
