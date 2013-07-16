/**
 * ====
 *     This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 *     Copyright (C) 2011-  SINTEF ICT
 *     Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 *     Module: net.modelbased.sensapp
 *
 *     SensApp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as
 *     published by the Free Software Foundation, either version 3 of
 *     the License, or (at your option) any later version.
 *
 *     SensApp is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General
 *     Public License along with SensApp. If not, see
 *     <http://www.gnu.org/licenses/>.
 * ====
 *
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.senml
 *
 * SensApp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SensApp is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with SensApp. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package net.modelbased.sensapp.library.senml.test.data

/**
 * SenML code snippets extracted from the standard description, version 08:
 * @url http://tools.ietf.org/html/draft-jennings-senml-08
 * @author Sebastien Mosser
 */
object SenMLSpecificationExamples {
  
  /**
   * The following shows a temperature reading taken approximately "now"
   * by a 1-wire sensor device that was assigned the unique 1-wire address
   * of 10e2073a01080063
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-6.1.1
   */
  val singleDatapoint = """
    {"e":[{ "n": "urn:dev:ow:10e2073a01080063", "v":23.5 , "u": "A"}]}
    """ // WARNING: deviation from the spec: unit is mandatory in our implementation
  
  /**
   * The following example shows voltage and current now, i.e., at an
   * unspecified time.  The device has an EUI-64 MAC address of
   * 0024befffe804ff1.
   * @url http://tools.ietf.org/html/draft-jennings-senml-08#section-6.1.2
   */
  val multipleDatapoint = """
    {"e":[
        { "n": "voltage", "t": 0, "u": "V", "v": 120.1 },
        { "n": "current", "t": 0, "u": "A", "v": 1.2 }],
     "bn": "urn:dev:mac:0024befffe804ff1/"
    }
    """
  
  /**
   * The next example is similar to the above one, but shows current at
   * Tue Jun 8 18:01:16 UTC 2010 and at each second for the previous 5
   * seconds.
   * @url http://tools.ietf.org/html/draft-jennings-senml-08#section-6.1.2
   */
  val multipleDatapointAndTime = """
    {"e":[
        { "n": "voltage", "u": "V", "v": 120.1 },
        { "n": "current", "t": -5, "v": 1.2 },
        { "n": "current", "t": -4, "v": 1.30 },
        { "n": "current", "t": -3, "v": 0.14e1 },
        { "n": "current", "t": -2, "v": 1.5 },
        { "n": "current", "t": -1, "v": 1.6 },
        { "n": "current", "t": 0,   "v": 1.7 }],
     "bn": "urn:dev:mac:0024befffe804ff1/",
     "bt": 1276020076,
     "ver": 1,
     "bu": "A"
    }
    """

  /**
   * The following example shows humidity measurements from a mobile
   * device with an IPv6 address 2001:db8::1, starting at Mon Oct 31 13:
   * 24:24 UTC 2011.  The device also provide position data, which is
   * provided in the same measurement or parameter array as separate
   * entries.  Note time is used to for correlating data that belongs
   * together, e.g., a measurement and a parameter associated with it.
   * Finally, the device also reports extra data about its battery status
   * at a separate time.  
   * @url http://tools.ietf.org/html/draft-jennings-senml-08#section-6.1.3
   */
  val multipleMeasurements = """
    {"e":[
        { "v": 20.0, "t": 0 },
        { "sv": "E 24' 30.621", "u": "lon", "t": 0 },
        { "sv": "N 60' 7.965", "u": "lat", "t": 0 },
        { "v": 20.3, "t": 60 },
        { "sv": "E 24' 30.622", "u": "lon", "t": 60 },
        { "sv": "N 60' 7.965", "u": "lat", "t": 60 },
        { "v": 20.7, "t": 120 },
        { "sv": "E 24' 30.623", "u": "lon", "t": 120 },
        { "sv": "N 60' 7.966", "u": "lat", "t": 120 },
        { "v": 98.0, "u": "%EL", "t": 150 },
        { "v": 21.2, "t": 180 },
        { "sv": "E 24' 30.628", "u": "lon", "t": 180 },
        { "sv": "N 60' 7.967", "u": "lat", "t": 180 }],
     "bn": "http://[2001:db8::1]",
     "bt": 1320067464,
     "bu": "%RH"
    }
    """
  
  /**
   * The following example shows how to query one device that can provide
   * multiple measurements.  The example assumes that a client has fetched
   * information from a device at 2001:db8::2 by performing a GET
   * operation on http://[2001:db8::2] at Mon Oct 31 16:27:09 UTC 2011,
   * and has gotten two separate values as a result, a temperature and
   * humidity measurement.
   * @url http://tools.ietf.org/html/draft-jennings-senml-08#section-6.1.4
   */
  val collectionOfResources = """
    {"e":[
        { "n": "temperature", "v": 27.2, "u": "degC" },
        { "n": "humidity", "v": 80, "u": "%RH" }],
     "bn": "http://[2001:db8::2]/",
     "bt": 1320078429,
     "ver": 1
    }
    """
}