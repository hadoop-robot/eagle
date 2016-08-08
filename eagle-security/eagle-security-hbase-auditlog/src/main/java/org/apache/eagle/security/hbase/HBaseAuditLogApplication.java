/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.hbase;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.security.topo.NewKafkaSourcedSpoutProvider;
import storm.kafka.StringScheme;

/**
 * Since 7/27/16.
 */
public class HBaseAuditLogApplication extends StormApplication<HBaseAuditLogAppConfig> {

    @Override
    public StormTopology execute(HBaseAuditLogAppConfig appConfig, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        NewKafkaSourcedSpoutProvider provider = new NewKafkaSourcedSpoutProvider();
        IRichSpout spout = provider.getSpout(appConfig.getConfig());
        HBaseAuditLogParserBolt bolt = new HBaseAuditLogParserBolt();

//        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
//        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
//        int numOfJoinTasks = config.getInt(JOIN_TASK_NUM);
//        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, appConfig.getSpoutTaskNum());
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", bolt, appConfig.getParserTaskNum());
        boltDeclarer.fieldsGrouping("ingest", new Fields(StringScheme.STRING_SCHEME_KEY));

        HbaseResourceSensitivityDataJoinBolt joinBolt = new HbaseResourceSensitivityDataJoinBolt(appConfig.getConfig());
        BoltDeclarer joinBoltDeclarer = builder.setBolt("joinBolt", joinBolt, appConfig.getJoinTaskNum());
        joinBoltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));

        StormStreamSink sinkBolt = environment.getFlattenStreamSink("hbase_audit_log_stream",appConfig.getConfig());
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, appConfig.getSinkTaskNum());
        kafkaBoltDeclarer.fieldsGrouping("joinBolt", new Fields("user"));
        return builder.createTopology();
    }

    public static void main(String[] args){
        Config config = ConfigFactory.load();
        HBaseAuditLogApplication app = new HBaseAuditLogApplication();
        app.run(config);
    }
}
