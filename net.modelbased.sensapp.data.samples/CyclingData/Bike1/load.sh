#!/bin/bash
#
# This file is part of SensApp [ http://sensapp.modelbased.net ]
#
# Copyright (C) 2011-  SINTEF ICT
# Contact: SINTEF ICT <nicolas.ferry@sintef.no>
#
# Module: net.modelbased.sensapp
#
# SensApp is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# SensApp is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General
# Public License along with SensApp. If not, see
# <http://www.gnu.org/licenses/>.
#

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