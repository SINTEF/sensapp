/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
import com.excilys.ebi.gatling.core.util.PathHelper.path2string
import com.excilys.ebi.gatling.recorder.configuration.CommandLineOptionsConstants.{ REQUEST_BODIES_FOLDER_OPTION, PACKAGE_OPTION, OUTPUT_FOLDER_OPTION }
import com.excilys.ebi.gatling.recorder.ui.GatlingHttpProxyUI

import IDEPathHelper.{ requestBodiesFolder, recorderOutputFolder }

object Recorder extends App {

	GatlingHttpProxyUI.main(Array(OUTPUT_FOLDER_OPTION, recorderOutputFolder, PACKAGE_OPTION, "net.modelbased.sensapp.backyard.gatling", REQUEST_BODIES_FOLDER_OPTION, requestBodiesFolder))
}