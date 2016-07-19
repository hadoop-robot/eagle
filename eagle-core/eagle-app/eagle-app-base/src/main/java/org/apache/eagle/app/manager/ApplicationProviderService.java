
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
package org.apache.eagle.app.manager;

import org.apache.eagle.app.Application;
import org.apache.eagle.app.ApplicationProvider;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.metadata.service.ApplicationDescService;

import java.util.Collection;

public interface ApplicationProviderService extends ApplicationDescService {
    void reload();
    Collection<ApplicationProviderConfig> getProviderConfigs();
    Collection<ApplicationProvider> getProviders();
    <T extends Application> ApplicationProvider<T> getApplicationProviderByType(String type);
}