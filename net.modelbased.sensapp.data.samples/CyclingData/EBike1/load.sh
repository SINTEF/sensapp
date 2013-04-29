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

## author: Sebastien Mosser

source ../../api.sh

for descr in `ls sensor_descriptors/EBike1-*.json`
do
    name=`cat $descr | grep id | cut -d ":" -f 2 | cut -d \" -f 2`
    register_sensor $name $descr
done

register_composite "EBike1" sensor_descriptors/EBike1.json

for file in `ls data/*.json`
do
    db_raw_import $file
done