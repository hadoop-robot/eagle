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

import backtype.storm.Config
import backtype.storm.utils.NimbusClient

import scala.collection.JavaConversions._


trait StreamTopologyManager {

  def getNimbusClient(config: Map[String, Object] = Map()): NimbusClient = {
    val nimbusConf = Map(Config.STORM_THRIFT_TRANSPORT_PLUGIN -> "backtype.storm.security.auth.SimpleTransportPlugin") ++ config
    val tConf = new Config() ++ nimbusConf
    NimbusClient.getConfiguredClient(tConf);
  }

  def submitTopology(topologyName: String)

  def updateTopology(appInput: String, stormConfig: Map[String, Object])

  def activateTopology(topology: String, stormConfig: Map[String, Object]) {
    getNimbusClient(stormConfig).getClient.activate(topology)
  }

  def deactivateTopology(topology: String, stormConfig: Map[String, Object]) {
    getNimbusClient(stormConfig).getClient.deactivate(topology)
  }

  def rebalanceTopology(topology: String, stormConfig: Map[String, Object]) {
    //TODO implement
    //getNimbusClient(stormConfig).getClient.rebalance(topology,null)
  }

  //TODO  give option for KillOption
  def killTopology(topology: String, stormConfig: Map[String, Object]) {
    getNimbusClient(stormConfig).getClient.killTopology(topology)
  }

}