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
package org.apache.eagle.tools.collector.input.impl;

import com.typesafe.config.Config;
import org.apache.eagle.tools.collector.input.LogStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 4/16/15
 */
public class MemoryLogStateManagerImpl implements LogStateManager {
    private Map<String,Long> logOffsets;
    private boolean isInited;
    private final static Logger LOG = LoggerFactory.getLogger(MemoryLogStateManagerImpl.class);

    @Override
    public void prepare(Config config) {
        if(isInited) {
            if(LOG.isDebugEnabled()) LOG.debug("MemoryLogOffsetManager is already initialized");
        }else {
            this.logOffsets = new HashMap<String, Long>();
            this.isInited = true;
        }
    }

    @Override
    public Long getLogOffset(String key){
        return this.logOffsets.containsKey(key)?this.logOffsets.get(key):0l;
    }

    @Override
    public Long setLogOffset(String key, long offset){
        this.logOffsets.put(key, offset);
        return offset;
    }

    @Override
    public boolean containsLogKey(String serverName){
        return this.logOffsets.containsKey(serverName);
    }
}