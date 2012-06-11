#!/bin/bash
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
