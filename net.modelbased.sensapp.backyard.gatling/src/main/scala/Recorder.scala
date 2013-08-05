import io.gatling.recorder.config.RecorderPropertiesBuilder
import io.gatling.recorder.controller.RecorderController

object Recorder extends App {

  val props = new RecorderPropertiesBuilder
  props.simulationOutputFolder(IDEPathHelper.recorderOutputDirectory.toString)
  props.simulationPackage("GatlingTest")
  props.requestBodiesFolder(IDEPathHelper.requestBodiesDirectory.toString)

  RecorderController(props.build, Some(IDEPathHelper.recorderConfigFile))
}