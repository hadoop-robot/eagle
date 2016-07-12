package org.apache.eagle.app.base.resource;

import org.apache.eagle.app.base.metadata.Site;
import org.apache.eagle.app.base.persistence.PersistenceManager;
import org.apache.eagle.app.base.repository.SiteRepository;

import javax.annotation.Resource;
import javax.ws.rs.Path;
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
@Resource
public class SiteResource {
    @Path("/v1/sites")
    public List<Site> getAllSites(){
        return PersistenceManager.getInstance().realize(SiteRepository.class).getAllSites();
    }
}