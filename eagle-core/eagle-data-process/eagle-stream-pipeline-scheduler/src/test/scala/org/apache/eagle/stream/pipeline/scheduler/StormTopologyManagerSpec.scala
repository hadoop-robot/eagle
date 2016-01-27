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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.stream.pipeline.Pipeline
import org.apache.eagle.stream.pipeline.scheduler.impl.StormTopologyManager
import org.apache.eagle.stream.scheduler.StreamAppConstants


object StormTopologyManagerSpec extends App {
  val config = ConfigFactory.load(StreamAppConstants.EAGLE_CONFIG_FILE)
  val manager: StormTopologyManager = new StormTopologyManager(config)

  val dataFlow = "\u0009dataflow {\n\u0009\u0009KafkaSource.metricStream_1 {\n\u0009\u0009\u0009parallism = 1000\n\u0009\u0009\u0009topic = \"metric_event_1\"\n\u0009\u0009\u0009zkConnection = \"sandbox.hortonworks.com:2181\"\n\u0009\u0009\u0009zkConnectionTimeoutMS = 15000\n\u0009\u0009\u0009consumerGroupId = \"Consumer\"\n\u0009\u0009\u0009fetchSize = 1048586\n\u0009\u0009\u0009transactionZKServers = \"sandbox.hortonworks.com\"\n\u0009\u0009\u0009transactionZKPort = 2181\n\u0009\u0009\u0009transactionZKRoot = \"/consumers\"\n\u0009\u0009\u0009transactionStateUpdateMS = 2000\n\u0009\u0009\u0009deserializerClass = \"org.apache.eagle.datastream.storm.JsonMessageDeserializer\"\n\u0009\u0009}\n\n\u0009\u0009KafkaSource.metricStream_2 {\n\u0009\u0009\u0009parallism = 1000\n\u0009\u0009\u0009topic = \"metric_event_2\"\n\u0009\u0009\u0009zkConnection = \"sandbox.hortonworks.com:2181\"\n\u0009\u0009\u0009zkConnectionTimeoutMS = 15000\n\u0009\u0009\u0009consumerGroupId = \"Consumer\"\n\u0009\u0009\u0009fetchSize = 1048586\n\u0009\u0009\u0009transactionZKServers = \"sandbox.hortonworks.com\"\n\u0009\u0009\u0009transactionZKPort = 2181\n\u0009\u0009\u0009transactionZKRoot = \"/consumers\"\n\u0009\u0009\u0009transactionStateUpdateMS = 2000\n\u0009\u0009\u0009deserializerClass = \"org.apache.eagle.datastream.storm.JsonMessageDeserializer\"\n\u0009\u0009}\n\n\u0009\u0009KafkaSource.metricStream_3{\n\u0009\u0009\u0009parallism = 1000\n\u0009\u0009\u0009topic = \"metric_event_3\"\n\u0009\u0009\u0009zkConnection = \"sandbox.hortonworks.com:2181\"\n\u0009\u0009\u0009zkConnectionTimeoutMS = 15000\n\u0009\u0009\u0009consumerGroupId = \"Consumer\"\n\u0009\u0009\u0009fetchSize = 1048586\n\u0009\u0009\u0009transactionZKServers = \"sandbox.hortonworks.com\"\n\u0009\u0009\u0009transactionZKPort = 2181\n\u0009\u0009\u0009transactionZKRoot = \"/consumers\"\n\u0009\u0009\u0009transactionStateUpdateMS = 2000\n\u0009\u0009\u0009deserializerClass = \"org.apache.eagle.datastream.storm.JsonMessageDeserializer\"\n\u0009\u0009}\n\n\u0009\u0009Console.printer {}\n\n\u0009\u0009metricStream_1|metricStream_2|metricStream_3 -> printer {\n\u0009\u0009\u0009grouping = shuffle\n\u0009\u0009}\n\u0009}"

  val dataFlowConf = "\u0009config {\n\u0009\u0009envContextConfig {\n\u0009\u0009\u0009\"env\" : \"storm\"\n\u0009\u0009\u0009\"mode\" : \"cluster\"\n\u0009\u0009\u0009\"topologyName\" : \"dsl-based-topology\"\n\u0009\u0009}\n\u0009\u0009eagleProps  {\n\u0009\u0009\u0009\"site\" : \"sandbox\"\n\u0009\u0009\u0009\"dataSource\": \"eventSource\"\n\u0009\u0009\u0009\"dataJoinPollIntervalSec\" : 30\n\u0009\u0009\u0009\"mailHost\" : \"mail.host.com\"\n\u0009\u0009\u0009\"mailSmtpPort\":\"25\"\n\u0009\u0009\u0009\"mailDebug\" : \"true\"\n\u0009\u0009\u0009\"eagleService\": {\n\u0009\u0009\u0009\u0009\"host\": \"localhost\"\n\u0009\u0009\u0009\u0009\"port\": 38080\n\u0009\u0009\u0009\u0009\"username\": \"admin\"\n\u0009\u0009\u0009\u0009\"password\": \"secret\"\n\u0009\u0009\u0009}\n\u0009\u0009}\n\u0009\u0009dynamicConfigSource  {\n\u0009\u0009\u0009\"enabled\" : true\n\u0009\u0009\u0009\"initDelayMillis\" : 0\n\u0009\u0009\u0009\"delayMillis\" : 30000\n\u0009\u0009}\n\u0009}"

  val pipeline = Pipeline.parseString(dataFlowConf + "\n" + dataFlow)
  val stream = Pipeline.compile(pipeline)

  def getClusterConfig(clusterName: String): Config = {
    val clusterConfigPath = StreamAppConstants.EAGLE_SCHEDULER_CONFIG + "." + clusterName
    if(!config.hasPath(clusterConfigPath)) {
      throw new Exception(s"Cannot find configuration under path: $clusterName")
    }
    config.getConfig(clusterConfigPath)
  }

  val clusterConfig = getClusterConfig("storm-lvs")
  val combinedClusterConfig = clusterConfig.withFallback(stream.getConfig)
  val ret = manager.start(stream, "test", combinedClusterConfig)
  println(ret)
}
