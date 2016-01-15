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
package org.apache.eagle.stream.dsl.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("appCommand")
@ColumnFamily("f")
@Prefix("appCommand")
@Service("AppCommandService")
@TimeSeries(false)
@Tags({"site", "uuid", "type"})
public class AppCommandEntity extends TaggedLogAPIEntity {

    @Column("a")
    private String appName;
    @Column("b")
    private String status;
    @Column("c")
    private long updateTimestamp;
    @Column("d")
    private long createTimestamp;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
        valueChanged("appName");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
        valueChanged("updateTimestamp");
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
        valueChanged("createTimestamp");
    }

    public final static class Type {
        public final static String START = "START";
        public final static String STOP = "STOP";
        public final static String RESTART = "RESTART";
    }

    public final static class Status {
        public final static String INITIALIZED = "INITIALIZED";
        public final static String RUNNING = "RUNNING";
        public final static String PENDING = "PENDING";
        public final static String DOWN = "DOWN";
    }
}