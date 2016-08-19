/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jobrunning.callback;

import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.crawler.JobContext;

import java.io.Serializable;
import java.util.List;

/**
 * callback when running job info is ready.
 */
public interface RunningJobCallback extends Serializable {

    /**
     * this is called when running job resource is ready.
     *
     * @param jobContext jobContext
     * @param type       type
     * @param objects    objects
     */
    void onJobRunningInformation(JobContext jobContext, JobConstants.ResourceType type, List<Object> objects);
}