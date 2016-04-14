package org.apache.eagle.tools.collector;

import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.eagle.tools.collector.input.LogInput;
import org.apache.eagle.tools.collector.output.LogOutput;

import java.util.ArrayList;
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
public class LogPipelineParser {
    private final static String TYPE_INSTANCE_FLAG_1=">";
    private final static String TYPE_INSTANCE_FLAG_2="<";
    private final static String INPUT_TYPE="input";
    private final static String OUTPUT_TYPE="output";

    public static class ParseException extends Exception {
        public ParseException(ReflectiveOperationException e) {
            super(e);
        }
    }

    static List<Pipeline> parse(Config config) throws ParseException {
        List<Pipeline> pipelines = new LinkedList<>();
        for(String pipelineName: config.root().keySet()){
            Pipeline pipeline = new Pipeline();
            pipeline.setName(pipelineName);
            Config singlePipelineConfig = config.getConfig(pipelineName);
            if(singlePipelineConfig.hasPath(INPUT_TYPE)) {
                pipeline.setInputs(parseInputs(singlePipelineConfig.getConfig(INPUT_TYPE)));
            }
            if(singlePipelineConfig.hasPath(OUTPUT_TYPE)) {
                pipeline.setOutputs(parseOutputs(singlePipelineConfig.getConfig(OUTPUT_TYPE)));
            }
            pipelines.add(pipeline);
        }
        return pipelines;
    }
    
    static List<LogInput> parseInputs(Config config) throws ParseException {
        List<LogInput> logInputs = new ArrayList<>();
        for(String name: config.root().unwrapped().keySet()){
            LogInput input;
            try {
                input = (LogInput) Class.forName(config.getConfig(name).getString("type")).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw  new ParseException(e);
            }
            input.init(config.getConfig(name),name);
            logInputs.add(input);
        }
        return logInputs;
    }

    private static List<LogOutput> parseOutputs(Config config) throws ParseException {
        List<LogOutput> logOutputs = new ArrayList<>();
        for(String name: config.root().unwrapped().keySet()){
            LogOutput output;
            try {
                output = (LogOutput) Class.forName(config.getConfig(name).getString("type")).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw  new ParseException(e);
            }
            output.init(config.getConfig(name),name);
            logOutputs.add(output);
        }
        return logOutputs;
    }
}