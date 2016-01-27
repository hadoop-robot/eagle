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
package org.apache.eagle.stream.pipeline.scheduler.impl

import com.typesafe.config.Config
import org.apache.eagle.stream.pipeline.Pipeline
import org.apache.eagle.stream.pipeline.scheduler.model.{StreamAppDefinition, StreamAppExecution}
import org.apache.eagle.stream.pipeline.scheduler.{StreamAppManager, StreamTopologyManager}
import org.apache.eagle.stream.scheduler.StreamAppConstants
import org.apache.eagle.stream.scheduler.entity.AppCommandEntity
import org.slf4j.LoggerFactory

class StreamAppManagerImpl(config: Config) extends StreamAppManager {
  private val LOG = LoggerFactory.getLogger(classOf[StreamAppManagerImpl])
  private var manager: StreamTopologyManager = _

  def getManager(streamAppDefinition:StreamAppDefinition): StreamTopologyManager = {
    return new StormTopologyManager(config)
  }

  def getClusterConfig(clusterName: String): Config = {
    val clusterConfigPath = StreamAppConstants.EAGLE_SCHEDULER_CONFIG + "." + clusterName
    if(!config.hasPath(clusterConfigPath)) {
      throw new Exception(s"Cannot find configuration under path: $clusterName")
    }
    config.getConfig(clusterConfigPath)
  }

  def execute(streamAppDefinition:StreamAppDefinition, streamAppExecution: StreamAppExecution): Boolean = {
    var ret = true
    manager = getManager(streamAppDefinition)
    val commandType = streamAppExecution.CommandType
    commandType match {
      case AppCommandEntity.Type.START => {
        try {
          val pipeline = Pipeline.parseString(streamAppDefinition.configuration + "\n" + streamAppDefinition.definition)
          val appName = streamAppDefinition.name
          val stream = Pipeline.compile(pipeline)
          val clusterConfig = getClusterConfig("storm-lvs")
          val combinedClusterConfig = clusterConfig.withFallback(stream.getConfig)
          ret = manager.start(stream, appName, combinedClusterConfig)
        } catch {
          case e: Throwable => {
            ret = false
          }
        }
      }
      case AppCommandEntity.Type.STOP => {
        try {
          val clusterConfig = getClusterConfig("storm-lvs")
          ret = manager.stop(streamAppDefinition.name, clusterConfig)
        } catch {
          case e: Throwable => {
            ret = false
          }
        }
      }
      case m@_ =>
        LOG.warn("Unsupported operation: " + commandType)
        ret = false
    }
    ret
  }

}