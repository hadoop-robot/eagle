package org.apache.eagle.tools.collector;

import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

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
public class LogPipelineParserTest {
    @Test
    public void parsePipeline() throws LogPipelineParser.ParseException {
        List<Pipeline> pipelineList = LogPipelineParser.parse(ConfigFactory.load().getConfig("eagle.collector"));
        Assert.assertEquals(2,pipelineList.size());
        Assert.assertEquals(1,pipelineList.get(0).getInputs().size());
        Assert.assertEquals(1,pipelineList.get(0).getOutputs().size());
        Assert.assertEquals(1,pipelineList.get(1).getInputs().size());
        Assert.assertEquals(1,pipelineList.get(1).getOutputs().size());
    }
}