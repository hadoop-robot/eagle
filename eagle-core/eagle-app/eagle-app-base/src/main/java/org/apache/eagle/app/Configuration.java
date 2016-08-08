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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.annotation.Property;
import org.apache.eagle.metadata.model.ApplicationEntity;

import java.io.Serializable;

public class Configuration implements Serializable {
    @Property("mode")
    private ApplicationEntity.Mode mode;

    @Property("siteId")
    private String siteId;

    @Property("appId")
    private String appId;

    @Property("jarPath")
    private String jarPath;

    private final Config config;

    public Configuration(Config config){
        this.config = config;
    }

    public ApplicationEntity.Mode getMode() {
        return mode;
    }

    public void setMode(ApplicationEntity.Mode mode) {
        this.mode = mode;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    @Deprecated
    public Config getConfig() {
        return config;
    }
}