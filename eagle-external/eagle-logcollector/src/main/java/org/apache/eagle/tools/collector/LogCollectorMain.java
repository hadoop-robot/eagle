package org.apache.eagle.tools.collector;

import com.typesafe.config.Config;
import org.apache.eagle.tools.collector.input.LogInput;
import org.apache.eagle.tools.collector.output.LogOutput;

import java.util.LinkedList;
import java.util.List;

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
public class LogCollectorMain {
    private final static String DEFAULT_LOG_PIPELINE_CONFIG_PREFIX = "eagle.collector";
    private final static String LOG_PIPELINE_CONFIG_PREFIX_ROOT = "eagle.collector.root";
    private final List<LogInput> inputList = new LinkedList<>();
    private final List<LogOutput> outputList = new LinkedList<>();

    public void prepare(Config config) {
        Config pipelineConfig;
        if(config.hasPath(LOG_PIPELINE_CONFIG_PREFIX_ROOT)) {
            pipelineConfig = config.getConfig(config.getString(LOG_PIPELINE_CONFIG_PREFIX_ROOT));
        } else {
            pipelineConfig = config.getConfig(DEFAULT_LOG_PIPELINE_CONFIG_PREFIX);
        }
    }

    public void run() {

    }
}