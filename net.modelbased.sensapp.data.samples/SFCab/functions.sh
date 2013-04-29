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

source ../api.sh

DATA_DIR="data"

# build_from_tpl cabId baseTime templateFile
function build_from_tpl {
    cat $3 | sed s/%NAME%/$1/ | sed s/%BT%/$2/
}

# extract_base_time senml_file
function extract_base_time {
    echo `cat $1 | cut -d , -f 2 | cut -d : -f 2`
}

# push cabId sensor
function push_sensor_data {
    FILE="$DATA_DIR/$1-$2.senml.json"
    BT=`extract_base_time $FILE`
    TMP=`mktemp -t load.sh.XXXXXX`
    trap "rm $TMP* 2>/dev/null" 0
    build_from_tpl $1 $BT sensor_descriptors/tpl-$2.json > $TMP
    register_sensor "sf/cab/$1/$2" $TMP
    db_raw_import $FILE
}

# push_composite cabId
function push_composite {
    push_sensor_data $1 "phi"
    push_sensor_data $1 "lambda"
    push_sensor_data $1 "occupied"
    TMP=`mktemp -t load.sh.XXXXXX`
    trap "rm $TMP* 2>/dev/null" 0
    build_from_tpl $1 "" sensor_descriptors/tpl-composite.json > $TMP
    register_composite $1 $TMP
}
