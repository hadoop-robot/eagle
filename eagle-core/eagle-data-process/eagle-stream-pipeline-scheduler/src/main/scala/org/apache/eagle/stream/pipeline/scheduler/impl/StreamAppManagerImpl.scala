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

import org.apache.eagle.stream.pipeline.scheduler.{StreamAppManager}
import org.apache.eagle.stream.pipeline.scheduler.model.{StreamAppExecution, StreamAppDefinition}
import org.apache.eagle.stream.scheduler.entity.AppCommandEntity
import org.slf4j.LoggerFactory

class StreamAppManagerImpl extends StreamAppManager {
  private val logger = LoggerFactory.getLogger(classOf[StreamAppManagerImpl])

  def execute(streamAppDefinition:StreamAppDefinition, streamAppExecution: StreamAppExecution): Boolean = {
    var ret = true
    val commandType = streamAppExecution.CommandType
    commandType match {
      case AppCommandEntity.Type.START => {
        try {
          ret = start(streamAppDefinition)
        } catch {
          case e: Throwable => {
            ret = false
          }
        }
      }
      case AppCommandEntity.Type.STOP => {
        try {
          ret = stop(streamAppDefinition)
        } catch {
          case e: Throwable => {
            ret = false
          }
        }
      }
      case AppCommandEntity.Type.RESTART => {
        try {
          ret = start(streamAppDefinition)
        } catch {
          case e: Throwable => {
            ret = false
          }
        }
      }
      case m@_ =>
        logger.warn("Unsupported operation: " + commandType)
        ret = false
    }
    ret
  }

  override def submit(app: StreamAppDefinition): Boolean = {
    return true
  }

  override def stop(app: StreamAppDefinition): Boolean = {
    return true
  }

  override def start(app: StreamAppDefinition): Boolean = {
    return true
  }

}