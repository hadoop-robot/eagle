/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.dsl.pipeline

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, FlatSpec}

class TestPipeline extends FlatSpec with Matchers{
  "pipeline" should "compile" in {
    val config = ConfigFactory.load(classOf[TestPipeline].getClassLoader,"pipeline")
    config should not be null
    val pipeline = Pipeline(config)
    pipeline should not be null
  }

  "pipeline" should "execute" in {
    val config = ConfigFactory.load(classOf[TestPipeline].getClassLoader,"pipeline")
    val pipeline = Pipeline(config)
    PipelineExecutor(pipeline).execute()
  }

  "config" should "parse successfully even when duplicated key" in {
    val config = ConfigFactory.parseString(
      """
        | some = [
        |   a -> b,
        |   b -> c,
        |   c -> d
        | ]
        |
      """.stripMargin)

      config should not be null
  }
}