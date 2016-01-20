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

import backtype.storm.{StormSubmitter, Config}
import backtype.storm.utils.NimbusClient
import org.apache.eagle.stream.pipeline.scheduler.StreamTopologyManager


class StormTopologyManager extends StreamTopologyManager {
  private var _nimbusClientOpt: Option[NimbusClient] = _

  private var _nimbusClient: NimbusClient = _

  private def initNimbusClient(stormConfig: Map[String, Object]) {
    _nimbusClient = _nimbusClientOpt match {
      case Some(nc) => nc
      case None     => getNimbusClient(stormConfig)
    }
  }

  override def updateTopology(appInput: String, stormConfig: Map[String, Object]): Unit = ???

  override def submitTopology(topologyName: String): Unit = {
    val (topologyName, conf, topology) = WordCountTopology.createWordCountTopology()
    StormSubmitter.submitTopology(topologyName, conf, topology)
  }
}
