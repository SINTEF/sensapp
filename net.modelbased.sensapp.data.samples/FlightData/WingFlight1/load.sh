#!/bin/bash
## author: Sebastien Mosser

source ../../api.sh

for descr in `ls sensor_descriptors/WingFlight1-*.json`
do
    name=`cat $descr | grep id | cut -d ":" -f 2 | cut -d \" -f 2`
    register_sensor $name $descr
done

register_composite "WingFlight1" sensor_descriptors/WingFlight1.json

for file in `ls data/*.json`
do
    db_raw_import $file
done