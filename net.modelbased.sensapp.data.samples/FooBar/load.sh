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






