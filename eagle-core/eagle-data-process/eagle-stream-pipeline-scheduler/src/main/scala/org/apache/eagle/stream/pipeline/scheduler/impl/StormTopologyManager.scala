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

import java.net.URLDecoder
import java.nio.file.{Paths, Files}

import backtype.storm.Config
import backtype.storm.utils.{NimbusClient, Utils}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.core.StreamContext
import org.apache.eagle.datastream.storm.StormExecutionEnvironment
import org.apache.eagle.stream.pipeline.scheduler.{StreamAppScheduler, StreamTopologyManager}
import org.apache.eagle.stream.scheduler.StreamAppConstants
import org.slf4j.LoggerFactory


class StormTopologyManager(schedulerConfig: com.typesafe.config.Config) extends StreamTopologyManager {
  val LOG = LoggerFactory.getLogger(classOf[StormTopologyManager])

  private def getNimbusClient(clusterConfig: com.typesafe.config.Config): NimbusClient = {
    val conf = Utils.readStormConfig().asInstanceOf[java.util.HashMap[String, Object]]
    conf.putAll(Utils.readCommandLineOpts().asInstanceOf[java.util.HashMap[String, Object]])
    conf.put(Config.NIMBUS_HOST, clusterConfig.getString(StreamAppConstants.EAGLE_STORM_NIMBUS))
    if(clusterConfig.hasPath(StreamAppConstants.EAGLE_STORM_NIMBUS_PORT)) {
      conf.put(Config.NIMBUS_THRIFT_PORT, clusterConfig.getNumber(StreamAppConstants.EAGLE_STORM_NIMBUS_PORT))
    }
    NimbusClient.getConfiguredClient(conf)
  }

  private def updateConfigWithTopologyName(appName: String, config: com.typesafe.config.Config): com.typesafe.config.Config = {
    var newConfig = config
    // support old setting
    if(newConfig.hasPath("envContextConfig.topologyName")) {
      newConfig.withValue("envContextConfig.topologyName", ConfigValueFactory.fromAnyRef(appName))
    } else {
      val topologyNameConfig = ConfigFactory.parseString(s"envContextConfig.topologyName=$appName")
      newConfig = newConfig.withFallback(topologyNameConfig)
    }
    newConfig
  }

  override def start(stream: StreamContext, appName: String, config: com.typesafe.config.Config): Boolean = {
    var ret = true
    try {
      val appConfig = updateConfigWithTopologyName(appName, config)
      // setting storm.jar
      var stormJarPath: String = URLDecoder.decode(classOf[StreamAppScheduler].getProtectionDomain.getCodeSource.getLocation.getPath,"UTF-8")
      if(appConfig.hasPath(StreamAppConstants.EAGLE_STORM_JARFILE)) {
        stormJarPath = appConfig.getString(StreamAppConstants.EAGLE_STORM_JARFILE)
      }
      if(stormJarPath == null ||  !Files.exists(Paths.get(stormJarPath)) || !stormJarPath.endsWith(".jar")) {
        val errMsg = s"storm jar file $stormJarPath does not exists, or is a invalid jar file"
        LOG.error(errMsg)
        throw new Exception(errMsg)
      }
      System.setProperty("storm.jar", stormJarPath)
      val stormEnv = ExecutionEnvironments.getWithConfig[StormExecutionEnvironment](appConfig)
      stream.submit(stormEnv)
    } catch {
      case e: Throwable =>
        ret = false
        LOG.error(e.toString)
    }
    ret
  }

  override def stop(topologyName: String, clusterConfig: com.typesafe.config.Config): Boolean = {
    var ret = true
    try {
      getNimbusClient(clusterConfig).getClient.killTopology(topologyName)
    } catch {
      case e: Throwable =>
        ret = false
        LOG.error(e.toString)
    }
    ret
  }

}
