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
package org.apache.eagle.service.client;

import org.apache.eagle.service.client.impl.*;

/**
 * IEagleServiceClient extension interfaces.
 *
 * @see IEagleServiceClient
 */
public interface IEagleServiceRequestBuilder {
    /**
     * Search pipe API.
     *
     * @return SearchRequestBuilder
     */
    SearchRequestBuilder search();

    /**
     * Search pipe API.
     *
     * @param query query text
     * @return SearchRequestBuilder
     */
    SearchRequestBuilder search(String query);

    /**
     * Delete pipe API.
     *
     * @return DeleteRequestBuilder
     */
    DeleteRequestBuilder delete();

    /**
     * Metric sender.
     *
     * @param metricName metricName
     * @return MetricSender
     */
    MetricSender metric(String metricName);

    /**
     * Batch entities sender.
     *
     * @param batchSize batchSize
     * @return BatchSender
     */
    BatchSender batch(int batchSize);

    /**
     * Async service client requester.
     *
     * @return EagleServiceAsyncClient
     */
    EagleServiceAsyncClient async();

    /**
     * Parallel service client requester.
     *
     * @param parallelNum parallelNum
     * @return ConcurrentSender
     */
    ConcurrentSender parallel(int parallelNum);


}