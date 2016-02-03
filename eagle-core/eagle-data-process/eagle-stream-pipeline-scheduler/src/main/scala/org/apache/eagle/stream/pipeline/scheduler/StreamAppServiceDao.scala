/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.pipeline.scheduler

import java.util
import java.util.concurrent.Callable

import akka.dispatch.Futures
import com.typesafe.config.Config
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.service.client.impl.EagleServiceClientImpl
import org.apache.eagle.stream.scheduler.StreamAppConstants
import org.apache.eagle.stream.scheduler.entity.{AppDefinitionEntity, AppCommandEntity}
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.ExecutionContext


class StreamAppServiceDao(config: Config, ex: ExecutionContext) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[StreamAppServiceDao])
  private val host: String = config.getString(StreamAppConstants.EAGLE_SERVICE_CONFIG + "." + EagleConfigConstants.HOST)
  private val port: Int = config.getInt(StreamAppConstants.EAGLE_SERVICE_CONFIG + "." + EagleConfigConstants.PORT)
  private val username: String = config.getString(StreamAppConstants.EAGLE_SERVICE_CONFIG + "." + EagleConfigConstants.USERNAME)
  private val password: String = config.getString(StreamAppConstants.EAGLE_SERVICE_CONFIG + "." + EagleConfigConstants.PASSWORD)

  def getEagleServiceClient(): EagleServiceClientImpl = {
    return new EagleServiceClientImpl(host, port, username, password)
  }

  def readNewInitializedCommandByType() = {
      Futures.future(new Callable[Option[util.List[AppCommandEntity]]]{
        override def call(): Option[util.List[AppCommandEntity]] = {
          val client = getEagleServiceClient()
          val query = "%s[@status=\"%s\"]{*}".format(StreamAppConstants.APP_COMMAND_SERVICE, AppCommandEntity.Status.INITIALIZED)
          val response: GenericServiceAPIResponseEntity[AppCommandEntity] = client.search(query).pageSize(Int.MaxValue).send()
          if(client != null) client.close()
          if(!response.isSuccess) {
            throw new RuntimeException(s"Got server side exception when executing query $query: ${response.getException}")
          }
          if(response.getObj != null && response.getObj.size() != 0) None else Option(response.getObj)
        }
      }, ex)
  }

  def readDefinitionByName(appName: String, site: String) = {
    Futures.future(new Callable[Option[AppDefinitionEntity]]{
      override def call(): Option[AppDefinitionEntity] = {
        val client = getEagleServiceClient()
        val query = "%s[@name=\"%s\" AND @site=\"%s\"]{*}".format(StreamAppConstants.APP_DEFINITION_SERVICE, appName, site)
        val response: GenericServiceAPIResponseEntity[AppDefinitionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(!response.isSuccess) {
          throw new RuntimeException(s"Got server side exception when executing query $query: ${response.getException}")
        }
        if(response.getObj != null && response.getObj.size() != 1) None else Option(response.getObj.get(0))
      }
    }, ex)
  }

  def updateCommandStatus(command: AppCommandEntity, status: String) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of command[$command] as $status")
        val client = getEagleServiceClient()
        command.setStatus(status)
        command.setUpdateTimestamp(System.currentTimeMillis())
        if(client != null) client.close()
        val response= client.update(util.Arrays.asList(command), classOf[AppCommandEntity])
        if(response.isSuccess) {
          LOG.info(s"Updated status command[$command] as: $status")
        } else {
          LOG.error(s"Failed to update status as $status of command[$command]")
          throw new RuntimeException(s"Failed to update command due to exception: ${response.getException}")
        }
        response
      }
    }, ex)
  }

  def updateDefinitionStatus(app: AppDefinitionEntity, status: String) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of app[$app] as $status")
        val client = getEagleServiceClient()
        app.setExecutionStatus(status)
        app.setUpdateTimestamp(System.currentTimeMillis())
        if(client != null) client.close()
        val response= client.update(util.Arrays.asList(app), classOf[AppDefinitionEntity])
        if(response.isSuccess) {
          LOG.info(s"Updated status application[$app] as: $status")
        } else {
          LOG.error(s"Failed to update status as $status of application[$app]")
          throw new RuntimeException(s"Failed to update application due to exception: ${response.getException}")
        }
        response
      }
    }, ex)
  }

}

