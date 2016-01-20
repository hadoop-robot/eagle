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

package org.apache.eagle.stream.pipeline.scheduler.impl

import java.util
import java.util.Random

import backtype.storm.task.TopologyContext
import backtype.storm.{Config}
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer, TopologyBuilder}
import backtype.storm.topology.base.{BaseBasicBolt, BaseRichSpout}
import backtype.storm.tuple.{Tuple, Fields, Values}
import backtype.storm.utils.Utils

import scala.collection.mutable


object WordCountTopology {

  class RandomSentenceSpout extends BaseRichSpout {
    private[scheduler] var _collector: SpoutOutputCollector = null
    private[scheduler] var _rand: Random = null

    def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector) {
      _collector = collector
      _rand = new Random
    }

    def nextTuple {
      Utils.sleep(100)
      val sentences: Array[String] = Array[String]("the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature")
      val sentence: String = sentences(_rand.nextInt(sentences.length))
      _collector.emit(new Values(sentence))
    }

    override def ack(id: AnyRef) {
    }

    override def fail(id: AnyRef) {
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("word"))
    }
  }

  class SplitSentence extends BaseBasicBolt {
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val sentence: String = tuple.getString(0)
      val words: Array[String] = sentence.split(" ")
      for (word <- words) {
        collector.emit(new Values(word))
      }
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("word"))
    }
  }

  class WordCount extends BaseBasicBolt {
    private[scheduler] var counts: mutable.HashMap[String, Integer] = new mutable.HashMap[String, Integer]

    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val word: String = tuple.getString(0)
      val count: Option[Integer] = counts.get(word)
      val cc = count match {
        case Some(c) => c+1
        case None => 0
      }
      counts.put(word, cc)
      collector.emit(new Values(word, count))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("word", "count"))
    }
  }

  def createWordCountTopology() = {
    val builder: TopologyBuilder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitSentence(), 2).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

    val conf: Config = new Config();
    conf.setDebug(true)
    conf.setNumWorkers(3)
    ("WordCount", conf, builder.createTopology())
  }

}
