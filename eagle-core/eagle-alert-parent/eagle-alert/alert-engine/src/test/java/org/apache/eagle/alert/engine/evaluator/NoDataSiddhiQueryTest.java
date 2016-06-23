package org.apache.eagle.alert.engine.evaluator;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
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
public class NoDataSiddhiQueryTest {
    @Test
    public void testNoDataAlertPolicyWithOuterJoin() throws InterruptedException {
        String[] allHosts = new String[]{"host_1","host_2","host_3","host_4","host_5","host_6","host_7","host_8"};
        String[] inHosts = new String[]{"host_6","host_7","host_8","host_6","host_7","host_8"};
        String[] noDataHosts = new String[]{"host_1","host_2","host_3","host_4","host_5"};

        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
                "define stream appearStream (key string, src string);"+
                "define stream allStream (key string,src string);"+
                "from allStream#window.length("+allHosts.length+")"+
                "left outer join appearStream#window.time(10 sec)" +
                "on allStream.key == appearStream.key "+
                "select allStream.key as h1,allStream.src as s1,appearStream.key as h2,appearStream.src as s2 insert into joinStream;"+
                "from joinStream[s2 is null] select h1 insert into missingStream"
        );

        runtime.addCallback("missingStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();

        for(String host:inHosts) {
            runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[]{host,"inStream"});
        }

        for(String host: allHosts) {
            runtime.getInputHandler("allStream").send(System.currentTimeMillis(), new Object[]{host,"allStream"});
        }

        Thread.sleep(5000);
    }

    @Test
    public void testNoDataAlertWithTable() throws InterruptedException {
        String[] expectHosts = new String[]{"host_1","host_2","host_3","host_4","host_5","host_6","host_7","host_8"};
        String[] appearHosts = new String[]{"host_6","host_7","host_8","host_6","host_7","host_8"};
        String[] noDataHosts = new String[]{"host_1","host_2","host_3","host_4","host_5"};

        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
            "define stream appearStream (key string, src string);"+
            "define stream expectStream (key string, src string);"+
            "define table expectTable (key string, src string);"+
            "define trigger fiveSecTriggerStream at every 1 sec;"+
            "from expectStream insert into expectTable;"+
            "from fiveSecTriggerStream join expectTable insert into triggerExpectStream;"+
            "from triggerExpectStream as l left outer join appearStream#window.time(5 sec) as r on l.key == r.key select l.key as k1,r.key as k2 insert current events into joinStream;" +
            "from joinStream[k2 is null] select k1 insert current events into missingStream;"
        );

        runtime.addCallback("missingStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();
        for(String host: expectHosts) {
            runtime.getInputHandler("expectStream").send(System.currentTimeMillis(), new Object[]{host,"expectStream"});
        }

        for(String host:appearHosts) {
            runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[]{host,"inStream"});
        }

        Thread.sleep(5000);

        for(String host:appearHosts) {
            runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[]{host,"inStream"});
        }
        Thread.sleep(10000);
    }
}
