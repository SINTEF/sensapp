#!/bin/bash
# author: Sebastien Mosser

## Loading the SensApp Bash API
source ../api.sh

THRESHOLD=30
WAIT=1
FILE=data/shuttle_all.senml.json 

## Registering sensors
register_sensor "chicago/uic/shuttle/phi" \
                sensor_descriptors/chicago_uic_shuttle_phi.json
register_sensor "chicago/uic/shuttle/lambda" \
                sensor_descriptors/chicago_uic_shuttle_lambda.json
register_composite "chicago/uic/shuttle" \
                   sensor_descriptors/chicago_uic_shuttle.json

## Pushing data
i=0
all=`wc -l $FILE | cut -d ' ' -f 3`
echo $all
while read line
do 
    i=$(($i + 1))
    cpt=$(( $i % $THRESHOLD ))
    if [ "X$cpt" = "X0" ]
    then
	remaining=$(($all - $i))
	echo "Done: $i - Remaining: $remaining"
	sleep $WAIT
    fi
    dispatch_data $line 
done < $FILE


