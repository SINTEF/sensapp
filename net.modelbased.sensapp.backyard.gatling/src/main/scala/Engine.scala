import com.excilys.ebi.gatling.app.{ Options, Gatling }
import com.excilys.ebi.gatling.core.util.PathHelper.path2string

object Engine extends App {

  args.length match {
    case 1 => { net.modelbased.sensapp.backyard.gatling.Target.serverName = args(0) }
  }
  
  new Gatling(Options(
		dataFolder = Some(IDEPathHelper.dataFolder),
		resultsFolder = Some(IDEPathHelper.resultsFolder),
		requestBodiesFolder = Some(IDEPathHelper.requestBodiesFolder),
		simulationBinariesFolder = Some(IDEPathHelper.mavenBinariesDir))).start
}