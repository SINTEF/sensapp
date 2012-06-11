# Shuttle location in Chicago

This dataset is published by the [Computer Science department of the University of Illinois at Chicago](http://www.cs.uic.edu/), available at the [following URL](http://www.cs.uic.edu/pub/Bits/Software/uic_shuttle_trips_110401_110430.zip). It contains 888 trips made by a shuttle in the UIC campus. 

## SensApp Sensor Declaration 

We consider the shuttle as a composite sensor named `chicago/uic/shuttle`, aggregating two atomic sensors (`chicago/uic/shuttle/phi` for latitude, and `chicago/uic/shuttle/lambda` for longitude).

The following description must be posted to the `/registry/sensors` endpoint of SensApp

    {
      "id": "chicago/uic/shuttle/phi", 
      "descr": "Latitude of the UIC shuttle",
      "schema": { "backend": "raw", "template": "Numerical", "baseTime": 1301806255}
    }  

    {
      "id": "chicago/uic/shuttle/lambda", 
      "descr": "Longitude of the UIC shuttle",
      "schema": { "backend": "raw", "template": "Numerical", "baseTime": 1301806255}
    }  


Now, the composite sensor can be declared as the following (using the `/registry/composite/sensors` endpoint)

    {
      "id": "chicago/uic/shuttle",
      "descr": "UIC Shuttle",
      "tags": { "source": "http://www.cs.uic.edu/pub/Bits/Software/uic_shuttle_trips_110401_110430.zip" },
      "sensors": ["/registry/sensors/chicago/uic/shuttle/lambda",
                  "/registry/sensors/chicago/uic/shuttle/phi"]
    }

## Loading the data into SensApp

The data sets are available in the `data` directory. Each file
corresponds to one trip of the shuttle. The script `load.sh` do the
trick, using the Bash SensApp API. 




