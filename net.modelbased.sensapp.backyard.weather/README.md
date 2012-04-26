Context
-------

The [Norwegian Meteorological Institute] provides free access to Norwegian 
meteorological data though the [Eklima] program.

We provide here a software library (written in Scala) to address the biggest 
method offered by this service, the retrieval of data for a given station 
located in Norway.

The library provides a small _domain specific language_ to address the service.


Installing the Library
----------------------

The library is exposed as a Maven artefact. The following dependency must be added:

    <dependency>
      <groupId>net.modelbased.sensapp.backyard</groupId>
  	  <artifactId>net.modelbased.sensapp.backyard.weather</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>

Using the DSL
-------------

The following code snippet retrieves the data available for the station #19980
(located in Lilleaker, west side of Oslo), between 01.01.2012 and 04.24.2012. The 
resulting SenML document is stored in the directory `./src/main/resources/`.

    import net.modelbased.sensapp.backyard.weather.EKlimaDSL
    object Main extends App with EKlimaDSL {
      19980.between("2012-01-01", "2012-04-24") -> "./src/main/resources/"
    }
    
Actually, this call is a wrapper to the [following invocation]([http://eklima.met.no/met/MetService?invoke=getMetData&timeserietypeID=0&format=&from=2012-01-01&to=2012-04-24&stations=19980&elements=RR%2CRRTA%2CTAM&hours=&months=&username=]), 
transforming the obtained XML data into standard SenML data.
    
    http://eklima.met.no/met/MetService?invoke=getMetData&timeserietypeID=0&format=&from=2012-01-01&to=2012-01-30&stations=18700&elements=RR%2CRRTA%2CTAM&hours=&months=&username=
    


  [Norwegian Meteorological Institute]: http://met.no/English/
  [Eklima]: http://eklima.met.no