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
package net.modelbased.sensapp.library.senml

sealed abstract case class Unit(val symbol: String, val description: String)

/**
 * IANA Unit scheme, extracted from the <a href="http://tools.ietf.org/html/draft-jennings-senml-07#section-10.1">SenML IETF standard</a>
 * @author Sebastien Mosser
 */
object IANA { 
  
  private val _definitions = Map(
    "m" -> meter,         "kg" -> kilogram,       "s" -> second,  "A" -> ampere, 
    "K" -> kelvin,        "cd" -> candela,        "mol" -> mole,  "Hz" -> hertz, 
    "rad" -> radian,      "sr" -> steradian,      "N" -> newton,  "Pa" -> pascal,
    "J" -> joule,         "W" -> watt,            "C" -> coulomb, "V" -> volt,  "F" -> farad,
    "Ohm" -> ohm,         "S" -> siemens,         "Wb" -> weber,  "T" -> tesla, "H" -> henry,
    "degC" -> degC,       "lm" -> lumen,          "lx" -> lux,    "Bq" -> becquerel,
    "Gy" -> gray,         "Sv" -> sievert,        "kat" -> katal, "%" -> `%`, 
    "count" -> count,     "%RH" -> `%RH`,         "m2" -> area,   "l" -> volume, 
    "m/s" -> velocity,    "m/s2" -> acceleration, "l/s" -> flow, 
    "W/m2" -> irradiance, "cd/m2" -> luminance,   "Bspl" -> belSound,
    "bit/s" -> bitrate,   "lat" -> lat,           "lon" -> lon, "%EL" -> `%EL`,
    "EL" -> EL, "beet/m" -> `beet/m`, "beets" -> beets, 
    
    "rad/s" -> `rad/s`,
    
    "g/km" -> `g/km`, "RPM" -> RPM, "l/100km" -> `l/100km`, "km/h" -> `km/h`
  )
   
  def apply(s: String): Option[Unit] = { _definitions get(s) }  
    
  object meter        extends Unit("m",     "meter") 
  object kilogram     extends Unit("kg",    "kilogram")
  object second       extends Unit("s",     "second")
  object ampere       extends Unit("A",     "ampere") 
  object kelvin       extends Unit("K",     "Kelvin")
  object candela      extends Unit("cd",    "candela")
  object mole         extends Unit("mol",   "mole")
  object hertz        extends Unit("Hz",    "Hertz")
  object radian       extends Unit("rad",   "radian")
  object steradian    extends Unit("sr",    "steradian")
  object newton       extends Unit("N",     "newton")
  object pascal       extends Unit("Pa",    "pascal")
  object joule        extends Unit("J",     "joule")
  object watt         extends Unit("W",     "Watt")
  object coulomb      extends Unit("C",     "coulomb")
  object volt         extends Unit("V",     "volt")
  object farad        extends Unit("F",     "farad")
  object ohm          extends Unit("Ohm",   "ohm")
  object siemens      extends Unit("S",     "siemens")
  object weber        extends Unit("Wb",    "Weber")
  object tesla        extends Unit("T",     "tesla")
  object henry        extends Unit("H",     "henry")
  object degC         extends Unit("degC",  "degrees Celsius")
  object lumen        extends Unit("lm",    "lumen") 
  object lux          extends Unit("lx",    "lux")
  object becquerel    extends Unit("Bq",    "becquerel")
  object gray         extends Unit("Gy",    "gray")
  object sievert      extends Unit("Sv",    "sievert")
  object katal        extends Unit("kat",   "katal")
  object `%`          extends Unit("%",     "value of a switch (0.0 means off, 100.0 means on)")
  object count        extends Unit("count", "value of a counter")
  object `%RH`        extends Unit("%RH",   "Relative Humidity")
  object area         extends Unit("m2",    "area")
  object volume       extends Unit("l",     "volume in liters")
  object velocity     extends Unit("m/s",   "velocity")
  object acceleration extends Unit("m/s2",  "acceleration")
  object flow         extends Unit("l/s",   "flow rate in liters per second")
  object irradiance   extends Unit("W/m2",  "irradiance")
  object luminance    extends Unit("cd/m2", "luminance")
  object belSound     extends Unit("Bspl",  "bel sound pressure level")
  object bitrate      extends Unit("bit/s", "bits per second")
  object lat          extends Unit("lat",   "degrees latitude, assumed to be in WGS84")
  object lon          extends Unit("lon",   "degrees longitude, assumed to be in WGS84")
  object `%EL`        extends Unit("%EL",   "remaining battery energy level in percents")
  object EL        extends Unit("EL",   "remaining battery energy level in seconds")
  object `beet/m`        extends Unit("beet/m",   "Heart rate in beets per minute")
  object beets       extends Unit("beets",   "Cumulative number of heart beats")
  
  object `rad/s` extends Unit("rad/s",   "Radians per second")
  /*
   * The following units are not defined in SenML08. They however fall in the extension mechanism defined in SenML08.
   */
  
  //SenML profile for the automotive industry
  object `g/km` extends Unit("g/km", "common measure for transportation pollution")
  object RPM extends Unit("RPM", "Revolutions Per Minute")
  object `l/100km` extends Unit("l/100km", "fuel consumption in liter per 100km")
  object `km/h` extends Unit("km/h", "speed in kilometer per hour")
}
