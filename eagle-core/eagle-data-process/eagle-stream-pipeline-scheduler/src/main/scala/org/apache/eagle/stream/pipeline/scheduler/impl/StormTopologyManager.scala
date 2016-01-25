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

import backtype.storm.generated.StormTopology
import com.typesafe.config.Config
import backtype.storm.{Config, StormSubmitter}
import backtype.storm.utils.{Utils, NimbusClient}
import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.core.StreamContext
import org.apache.eagle.datastream.storm.StormExecutionEnvironment
import org.apache.eagle.stream.pipeline.scheduler.StreamTopologyManager
import org.apache.eagle.stream.scheduler.AppConstants
import org.slf4j.LoggerFactory


class StormTopologyManager(schedulerConfig: com.typesafe.config.Config) extends StreamTopologyManager {
  val LOG = LoggerFactory.getLogger(classOf[StormTopologyManager])

  private def getNimbusClient(clusterConfig: com.typesafe.config.Config): NimbusClient = {
    val conf = Utils.readStormConfig().asInstanceOf[java.util.HashMap[String, Object]]
    conf.putAll(Utils.readCommandLineOpts().asInstanceOf[java.util.HashMap[String, Object]])
    conf.put(backtype.storm.Config.NIMBUS_HOST, clusterConfig.getString(AppConstants.EAGLE_STORM_NIMBUS))
    NimbusClient.getConfiguredClient(conf)
  }

  override def start(stream: StreamContext, topologyName: String, clusterConfig: com.typesafe.config.Config): Boolean = {
    var ret = true
    try {
      //val (topologyName, conf, topology) = WordCountTopology.createWordCountTopology()
      val stormEnv = ExecutionEnvironments.getWithConfig[StormExecutionEnvironment](clusterConfig)
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
