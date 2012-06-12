#!/bin/bash
# author: Sebastien Mosser

## Loading the SensApp bash API
source ../../api.sh

## Registering sensors
register_sensor "Bike1/altitude" sensor_descriptors/Bike1-altitude.json
register_sensor "Bike1/crs" sensor_descriptors/Bike1-crs.json
register_sensor "Bike1/gps_alt" sensor_descriptors/Bike1-gps_alt.json
register_sensor "Bike1/gps_fix" sensor_descriptors/Bike1-gps_fix.json
register_sensor "Bike1/gpssats" sensor_descriptors/Bike1-gpssats.json
register_sensor "Bike1/ground_speed" sensor_descriptors/Bike1-ground_speed.json
register_sensor "Bike1/heading" sensor_descriptors/Bike1-heading.json
register_sensor "Bike1/latitude" sensor_descriptors/Bike1-latitude.json
register_sensor "Bike1/longitude" sensor_descriptors/Bike1-longitude.json
register_sensor "Bike1/pitch" sensor_descriptors/Bike1-pitch.json
register_sensor "Bike1/roll" sensor_descriptors/Bike1-roll.json
register_sensor "Bike1/sonar" sensor_descriptors/Bike1-sonar.json

## Declaring composite sensor
register_composite "Bike1" sensor_descriptors/Bike1.json

## Loading data
for file in `ls data`
do
    db_raw_import data/$file
done