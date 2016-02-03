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
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.stream.scheduler.StreamAppConstants
import org.apache.eagle.stream.scheduler.entity.{AppCommandEntity, AppDefinitionEntity}

import scala.collection.JavaConversions
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


private[scheduler] class ScheduleEvent
private[scheduler] case class InitializationEvent(config: Config) extends ScheduleEvent
private[scheduler] case class TerminatedEvent() extends ScheduleEvent
private[scheduler] case class CommandLoaderEvent() extends ScheduleEvent
private[scheduler] case class CommandExecuteEvent(appCommandEntity: AppCommandEntity) extends ScheduleEvent
private[scheduler] case class ExecutionCommand(command: AppCommandEntity) extends ScheduleEvent
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
  val config = ConfigFactory.load(StreamAppConstants.EAGLE_CONFIG_FILE)

  def start():Unit = {
    val system = ActorSystem(config.getString(StreamAppConstants.EAGLE_SCHEDULER_CONFIG + "." + StreamAppConstants.SCHEDULE_SYSTEM), config)

    system.log.info(s"Started actor system: $system")

    import system.dispatcher

    val coordinator = system.actorOf(Props[StreamAppCoordinator])

    system.scheduler.scheduleOnce(1 seconds, coordinator, InitializationEvent(config))

    system.scheduler.schedule(1.seconds, 5.seconds, coordinator, CommandExecuteEvent)

    /*
     registerOnTermination is called when you have shut down the ActorSystem (system.shutdown),
     and the callbacks will be executed after all actors have been stopped.
     */
    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! TerminatedEvent
      }
    })

  }
}

private[scheduler] class StreamAppCoordinator extends Actor with ActorLogging {

  var commandLoader: ActorRef = null
  var commandExecutor: ActorRef = null

  override def preStart(): Unit = {
    commandLoader = context.actorOf(Props[StreamAppCommandLoader], "command-loader")
    commandExecutor = context.actorOf(Props[StreamAppCommandExecutor], "command-worker")
  }

  override def receive = {
    case InitializationEvent(config) => {
      log.info(s"Config updated: $config")
      commandLoader ! InitializationEvent(config)
      commandExecutor ! InitializationEvent(config)
    }
    case CommandLoaderEvent =>
      commandLoader ! CommandExecuteEvent
    case ExecutionCommand(command) =>
      log.info(s"Executing comamnd: $command")
      commandExecutor ! ExecutionCommand(command)
    case TerminatedEvent =>
      log.info("Coordinator exit ...")
      context.stop(self)
    case m@_ =>
      log.warning(s"Unsupported message: $m")
  }
}

private[scheduler] class StreamAppCommandLoader extends Actor with ActorLogging {
  @volatile var _config: Config = null
  @volatile var _dao: StreamAppServiceDao = null

  import context.dispatcher

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new StreamAppServiceDao(config, context.dispatcher)

    case CommandLoaderEvent => {
      val _sender = sender()
      _dao.readNewInitializedCommandByType() onComplete {
        case Success(optionalEntities) =>
          optionalEntities match {
            case Some(commands) =>
              log.info(s"Load ${commands.size()} new commands")
              JavaConversions.collectionAsScalaIterable(commands) foreach { command =>
                _dao.updateCommandStatus(command, AppCommandEntity.Status.PENDING) onComplete {
                  case Success(response) =>
                    if(response.isSuccess) {
                      _sender ! ExecutionCommand(command)
                    } else {
                      log.error(s"Got an exception to update command $command: ${response.getException}")
                    }
                  case Failure(ex) =>
                    log.error(s"Got an exception to update command $command: ${ex.getMessage}")
                }
              }
            case None =>
              log.info("Load 0 new commands")
          }
        case Failure(ex) =>
          log.error(s"Failed to get commands due to exception ${ex.getMessage}")
      }
    }

    case TerminatedEvent =>
      context.stop(self)

    case m@_ => throw new UnsupportedOperationException(s"Event is not supported $m")
  }
}

private[scheduler] class StreamAppCommandExecutor(config: Config) extends Actor with ActorLogging {
  @volatile var _config: Config = _
  @volatile var _dao: StreamAppServiceDao = _
  @volatile var _streamAppManager: StreamAppManager = _

  import context.dispatcher

  def updateCompletedStatus(ret: Boolean) = {
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
    (newAppStatus, newCmdStatus)
  }

  def execute(app: AppDefinitionEntity, command: AppCommandEntity) {
    val streamAppDefinition = AppDefinitionEntity.toModel(app)
    val streamAppCommand = AppCommandEntity.toModel(command)
    val future = Future {
      _streamAppManager.execute(streamAppDefinition, streamAppCommand)
    }
    future.onComplete {
      case Success(value) =>
        val (appStatus, commandStatus) = updateCompletedStatus(value)
        _dao.updateDefinitionStatus(app, appStatus) recover {
          case ex: Throwable => log.error(ex,s"Failed to update executed status of $app due to exception: ${ex.getMessage}")
        }
        _dao.updateCommandStatus(command, commandStatus) recover {
          case ex: Throwable => log.error(ex,s"Failed to update executed status of $app due to exception: ${ex.getMessage}")
        }
      case Failure(ex) =>
        log.error(ex,s"Failed to execute command $command on application $app due to exception: ${ex.getMessage}")
    }
  }

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new StreamAppServiceDao(config, context.dispatcher)
      _streamAppManager = StreamAppManager.apply(config)

    case CommandExecuteEvent(command) => {
      val streamAppExecution = AppCommandEntity.toModel(command)
      val appName = streamAppExecution.appName
      val site = streamAppExecution.site

      _dao.readDefinitionByName(appName, site) onComplete {
        case Success(appDefinition) =>
          appDefinition match {
            case Some(app) =>
              _dao.updateDefinitionStatus(app, AppDefinitionEntity.STATUS.STARTING) onComplete {
                case _ => execute(app, command)
              }

            case None =>
              log.error(s"Failed to find application definition with applicationName=$appName and site=$site")
          }

      }
    }

    case m@_ =>
      log.warning("Unsupported operation $m")
  }
}


object StreamAppScheduler extends App {
  new StreamAppScheduler().start()
}
