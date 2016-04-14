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
package org.apache.eagle.tools.collector.input;

import com.typesafe.config.Config;

/**
 * TODO: persist offset status for distributed environment through persisted storage or kafka, or make sure the logs from the same server are always sent to the same executor instance (say "bolt" for storm)
 *
 * @since 4/16/15
 */
public interface LogStateManager {
    void prepare(Config config);
    /**
     *
     * @param key
     * @return
     */
    Long getLogOffset(String key);

    /**
     *
     * @param key
     * @param offset
     */
    Long setLogOffset(String key, long offset);

    /**
     *
     * @param key
     * @return
     */
    boolean containsLogKey(String key);
}