#!/bin/bash

#SRV="demo.sensapp.org"
#HOST=80
SRV="127.0.0.1"
HOST=8080

SENSAPP_REGISTRY="http://$SRV:$HOST/sensapp"
SENSAPP_DATABASE_RAW="http://$SRV:$HOST/sensapp"
SENSAPP_DISPATCH="http://$SRV:$HOST/sensapp"

###             ###
# Sensor Registry #
###             ###

# register_sensor NAME DESCRIPTION_FILE
function register_sensor {
    echo -e "Registering sensor [$1]"
    curl -X POST -d "@$2" \
         --header "Content-Type: application/json" \
         $SENSAPP_REGISTRY/registry/sensors
    echo -e "\n"
}

# update_sensor_info NAME DESCRIPTION_FILE
function update_sensor_info {
    echo -e "Updating sensor [$1]"
    curl -X PUT -d "@$2" \
         --header "Content-Type: application/json" \
         $SENSAPP_REGISTRY/registry/sensors/$1 
    echo -e "\n"
}


# register_composite NAME DESCRIPTION_FILE
function register_composite {
    echo -e "Registering Composite [$1]"
    curl -X POST -d "@$2" \
	--header "Content-Type: application/json" \
	$SENSAPP_REGISTRY/registry/composite/sensors 
    echo -e "\n"  
}


###             ###
# Database :: RAW #
###             ###

function db_raw_push {
    echo -e "Pushing data into SensApp [$1]"
    curl -X PUT -d "@$2" \
	--header "Content-Type: application/json" \
	$SENSAPP_DATABASE_RAW/databases/raw/data/$1 
    echo -e "\n"  
}

function db_raw_import {
    echo -e "Loading data into SensApp [$1]"
    curl -X PUT -d "@$1" \
	--header "Content-Type: application/json" \
	$SENSAPP_DATABASE_RAW/databases/raw/load
    echo -e "\n"  
}

###
# Dispatcher
###

function dispatch_data {
    echo -e "Pushing data [$1]"
    curl -X PUT -d "$1" \
	--header "Content-Type: application/json" \
	$SENSAPP_DISPATCH/dispatch
    echo -e "\n"
}

# dispatch FILE_CONTAINING_SENML_DATA
function dispatch {
    echo -e "Pushing data [$1]"
    curl -X PUT -d "@$1" \
	--header "Content-Type: application/json" \
	$SENSAPP_DISPATCH/dispatch
    echo -e "\n"
}
