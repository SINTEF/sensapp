#!/bin/bash
# author: Sebastien Mosser

## Loading the SensApp Bash API
source  ../api.sh

## Registering sensors
register_sensor "my-sensor/inside" \
                sensor_descriptors/mySensor_inside.json
update_sensor_info "my-sensor/inside" \
                   sensor_descriptors/mySensor_inside_infos.json

register_sensor "my-sensor/outside" \
                sensor_descriptors/mySensor_outside.json

## Pushing data into SensApp
dispatch data/dummy.senml.json






