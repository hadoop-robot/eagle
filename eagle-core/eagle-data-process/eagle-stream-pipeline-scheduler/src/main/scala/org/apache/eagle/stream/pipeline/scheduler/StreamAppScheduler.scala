/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.pipeline.scheduler

import akka.actor._
import akka.routing.RoundRobinRouter
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.stream.scheduler.AppConstants
import org.apache.eagle.stream.scheduler.dao.AppEntityDaoImpl
import org.apache.eagle.stream.scheduler.entity.{AppCommandEntity, AppDefinitionEntity}
import scala.collection.JavaConverters._


private[scheduler] class ScheduleEvent
private[scheduler] case class InitializationEvent() extends ScheduleEvent
private[scheduler] case class TerminatedEvent() extends ScheduleEvent
private[scheduler] case class CommandLoaderEvent() extends ScheduleEvent
private[scheduler] case class CommandExecuteEvent(appCommandEntity: AppCommandEntity) extends ScheduleEvent
private[scheduler] case class HealthCheckerEvent() extends ScheduleEvent
private[scheduler] case class ResultCountEvent(value: Int) extends ScheduleEvent

case class EagleServiceUnavailableException(message:String) extends Exception(message)
case class DuplicatedDefinitionException(message:String) extends Exception(message)


/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
private[scheduler] class StreamAppScheduler() {
  //System.setProperty("config.resource", "/application.local.conf")
  val config = ConfigFactory.load(AppConstants.EAGLE_CONFIG_FILE)

  def start():Unit = {
    val system = ActorSystem(config.getString(AppConstants.EAGLE_SCHEDULER_CONFIG + "." + AppConstants.SCHEDULE_SYSTEM), config)
    system.log.info(s"Started actor system: $system")

    val coordinator = system.actorOf(Props(new StreamAppCoordinator(config)))
    coordinator ! InitializationEvent

  }
}

private[scheduler] class StreamAppCoordinator(config: Config) extends Actor with ActorLogging {
  private val loader: ActorRef = context.actorOf(Props(new StreamAppCommandLoader(config)), "command-loader")

  override def receive = {
    case InitializationEvent => {
      loader ! InitializationEvent
      loader ! CommandLoaderEvent
    }

    case TerminatedEvent =>
      context.stop(self)

    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[scheduler] class StreamAppCommandLoader(config: Config) extends Actor with ActorLogging {

  def loadCommands(): GenericServiceAPIResponseEntity[_] = {
    val dao = new AppEntityDaoImpl(config)
    val query = "AppCommandService[@status=\"%s\"]{*}".format(AppCommandEntity.Status.INITIALIZED)
    dao.search(query, Int.MaxValue)
  }

  var progressListener: Option[ActorRef] = None
  val configPath = AppConstants.EAGLE_SCHEDULER_CONFIG + "." + AppConstants.SCHEDULE_NUM_WORKERS
  var numOfWorkers = 1
  if(config.hasPath(configPath)) {
    numOfWorkers = config.getInt(configPath)
  }
  val workerRouter = context.actorOf(
    Props(new StreamAppCommandExecutor(config)).withRouter(RoundRobinRouter(numOfWorkers)), name = "command-executor")

  override def receive = {
    case InitializationEvent if progressListener.isEmpty =>
      progressListener = Some(sender())

    case CommandLoaderEvent => {
      val response = loadCommands()
      //println("response" + response.getObj.toString)
      if(response.getException != null)
        throw new EagleServiceUnavailableException(s"Service is unavailable" + response.getException)

      val commands = response.getObj().asScala
      if(commands != null && commands.size != 0){
        val appCommands = commands.toList
        for(command <- appCommands) {
          val cmd = command.asInstanceOf[AppCommandEntity]
          cmd.setStatus(AppCommandEntity.Status.PENDING)
          workerRouter ! CommandExecuteEvent(cmd)
        }
      }
    }
    case TerminatedEvent =>
      context.stop(self)

    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[scheduler] class StreamAppCommandExecutor(config: Config) extends Actor with ActorLogging {
  val dao = new AppEntityDaoImpl(config)
  val streamAppManager = StreamAppManager.apply()

  def loadAppDefinition(appName: String, site: String): GenericServiceAPIResponseEntity[_] = {
    val query = "AppDefinitionService[@name=\"%s\" AND @site=\"%s\"]{*}".format(appName, site)
    println(query)
    dao.search(query, Integer.MAX_VALUE)
  }

  def changeAppStatus(app: AppDefinitionEntity, newStatus: String): Unit = {
    app.setExecutionStatus(newStatus)
    dao.update(app, AppConstants.APP_DEFINITION_SERVICE)
  }

  def changeCommandStatus(cmd: AppCommandEntity, newStatus: String): Unit = {
    cmd.setStatus(newStatus)
    dao.update(cmd, AppConstants.APP_COMMAND_SERVICE)
  }

  def updateCompletedStatus(ret: Boolean, appDefinition: AppDefinitionEntity, appCommand: AppCommandEntity) = {
    var newAppStatus: String = AppDefinitionEntity.STATUS.UNKNOWN
    var newCmdStatus: String = AppCommandEntity.Status.PENDING
    ret match {
      case true => {
        newAppStatus = AppDefinitionEntity.STATUS.RUNNING
        newCmdStatus = AppCommandEntity.Status.RUNNING
      }
      case m@_ => {
        newAppStatus = AppDefinitionEntity.STATUS.STOPPED
        newCmdStatus = AppCommandEntity.Status.DOWN
      }
    }
    changeAppStatus(appDefinition, newAppStatus)
    changeCommandStatus(appCommand, newCmdStatus)
  }


  override def receive = {
    case CommandExecuteEvent(command) => {
      val streamAppExecution = AppCommandEntity.toModel(command)
      val appName = streamAppExecution.appName
      val site = streamAppExecution.site

      val result = loadAppDefinition(appName, site)
      println("result=" + result.getObj.toString)

      if(result.getException != null) {
        throw new EagleServiceUnavailableException("Service is not available" + result.getException)
      }

      val definitions = result.getObj()
      if(definitions != null && definitions.size != 1) {
        log.error(s"The AppDefinitionService result searched by name $appName is not correct")
        throw new DuplicatedDefinitionException(s"The AppDefinitionService result searched by name $appName is not correct")
      }

      val appDefinition = definitions.get(0).asInstanceOf[AppDefinitionEntity]
      val streamAppDefinition = AppDefinitionEntity.toModel(appDefinition)
      var ret = true
      try {
        changeAppStatus(appDefinition, AppDefinitionEntity.STATUS.STARTING)
        ret = streamAppManager.execute(streamAppDefinition, streamAppExecution)
        updateCompletedStatus(ret, appDefinition, command)
      } catch {
        case e: Throwable =>
          throw new UnsupportedOperationException(s"Execute command failed $e")
      }
    }
    case m@_ =>
      log.warning("Unsupported operation $m")
  }
}


object StreamAppScheduler extends App {
  new StreamAppScheduler().start()
}
