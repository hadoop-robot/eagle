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
package org.apache.eagle.tools.collector;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.tools.collector.input.HadoopLogWebTailer;
import org.apache.eagle.tools.collector.input.HostConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @since 4/16/15
 */
public class HadoopServiceLogInputTest {
    private Config config;
    private HadoopLogWebTailer tailer;
    private Config pipelineConfig;

    @Before
    public void setUp() {
        tailer = new HadoopLogWebTailer();
        config = ConfigFactory.load();
        pipelineConfig = config.getConfig("eagle.collector.hdfsAuditLogCollector_1.input");
        tailer.init(pipelineConfig,"HadoopServiceLogReaderTest");
    }

    @Test
    public void testLogFileContext(){
        HostConfig log = new HostConfig();
        log.setHostName("localhost");
        log.setInfoPort(60030);
        assert log.format("%hostname%--%infoport%").equals("localhost--60030");
    }

    @Test
    public void testLogCollect() throws InterruptedException, IOException {
        int round=0;
        while(round < 3) {
            tailer.next();
            round ++;
            Thread.sleep(1);
        }
    }
}