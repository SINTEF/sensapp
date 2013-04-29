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


source functions.sh

WAIT=1


if [ "$#" == "1" ]
then
    IDS=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | sort | head -n $1`
else
    IDS=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | sort`
fi

echo $IDS


ALL=`ls $DATA_DIR | cut -d "-" -f 1 | uniq | wc -l | tr -s " "`

CPT=0
for CAB_ID in $IDS
do
    CPT=$(($CPT + 1))
    echo -e "###\n# $CAB_ID [$CPT/$ALL]\n###"
    push_composite $CAB_ID
    sleep $WAIT
done

