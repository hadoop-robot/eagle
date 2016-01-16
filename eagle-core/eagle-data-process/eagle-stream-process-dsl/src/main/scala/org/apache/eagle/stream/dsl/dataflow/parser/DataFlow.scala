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
package org.apache.eagle.stream.dsl.dataflow.parser

import com.typesafe.config.Config

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

class DataFlow {
  private var processors = mutable.Map[String,Processor]()
  private var connectors = mutable.Seq[Connector]()
  def setProcessors(processors:Seq[Processor]):Unit = {
    processors.foreach{module =>
      this.processors.put(module.getId,module)
    }
  }
  def setProcessors(processors:mutable.Map[String,Processor]):Unit = {
    this.processors = processors
  }
  def setConnectors(connectors:Seq[Connector]):Unit = {
    connectors.foreach(connector =>{
      this.connectors :+= connector
    })
  }
  def addProcessor(module:Processor):Unit = {
    if(contains(module)) throw new IllegalArgumentException(s"Duplicated processor id error, ${module.getId} has already been defined as ${getProcessor(module.getId)}")
    processors.put(module.getId,module)
  }

  def contains(module:Processor):Boolean = processors.contains(module.getId)
  def addConnector(connector:Connector):Unit = {
    connectors :+= connector
  }
  def getProcessors:Seq[Processor] = processors.values.toSeq
  def getProcessor(processorId:String):Option[Processor] = processors.get(processorId)
  def getConnectors:Seq[Connector] = connectors
}

case class Processor(var processorId:String = null,var processorType:String = null,var schema:Schema = null, var processorConfig:Map[String,AnyRef] = null) extends Serializable {
  def getId:String = processorId
  def getType:String = processorType
  def getConfig:Map[String,AnyRef] = processorConfig
  def getSchema:Option[Schema] = if(schema == null) None else Some(schema)
}

case class Connector (from:String,to:String, config:Map[String,AnyRef]) extends Serializable


private [dataflow]
object Processor {
  val SCHEMA_FIELD:String = "schema"
  def parse(processorId:String,processorType:String,context:Map[String,AnyRef], schemaSet:SchemaSet):Processor = {
    val schema = context.get(SCHEMA_FIELD) match {
      case Some(schemaDef) => schemaDef match {
        case schemaId:String => schemaSet.get(schemaId).getOrElse {
          throw new ParseException(s"Schema [$schemaId] is not found but referred by [$processorType:$processorId] in $context")
        }
        case schemaMap:java.util.HashMap[String,AnyRef] => Schema.parse(schemaMap.toMap)
        case _ => throw new ParseException(s"Illegal value for schema: $schemaDef")
      }
      case None => null
    }
    new Processor(processorId,processorType,schema,context-SCHEMA_FIELD)
  }
}


trait DataFlowParser {
  def parse(config:Config,schemaSet:SchemaSet = SchemaSet.empty()):DataFlow = {
    val dataw = new DataFlow()
    val map = config.root().unwrapped().toMap

    // Parse processors and connectors
    map.foreach(entry => {
      parseSingleProcessor(entry._1,entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap,dataw,schemaSet)
    })

    validate(dataw)
    dataw
  }

  private def
  validate(pipeline:DataFlow): Unit ={
    def checkModuleExists(id:String): Unit ={
      pipeline.getProcessor(id).orElse {
        throw new ParseException(s"Stream [$id] is not defined before being referred")
      }
    }

    pipeline.getConnectors.foreach {connector =>
      checkModuleExists(connector.from)
      checkModuleExists(connector.to)
    }
  }

  private def
  parseSingleProcessor(identifier:String,config:Map[String,AnyRef],dataflow:DataFlow, schemaSet: SchemaSet):Unit = {
    Identifier.parse(identifier) match {
      case DefinitionIdentifier(processorType) => {
        config foreach {entry =>
          dataflow.addProcessor(Processor.parse(entry._1, processorType,entry._2.asInstanceOf[java.util.HashMap[String, AnyRef]].toMap,schemaSet))
        }
      }
      case ConnectionIdentifier(fromIds,toId) => fromIds.foreach { fromId =>
        if(fromId.eq(toId)) throw new ParseException(s"Can't connect $fromId to $toId")
        dataflow.addConnector(Connector(fromId,toId,config))
      }
      case _ => ???
    }
  }
}


private[dataflow] trait Identifier

private[dataflow] case class DefinitionIdentifier(moduleType: String) extends Identifier
private[dataflow] case class ConnectionIdentifier(fromIds: Seq[String], toId: String) extends Identifier

private[dataflow] object Identifier {
  val ConnectorFlag = "->"
  val UnitFlagSplitPattern = "\\|"
  val UnitFlagChar = "|"
  val ConnectorPattern = s"([\\w-|\\s]+)\\s+$ConnectorFlag\\s+([\\w-_]+)".r
  def parse(identifier: String): Identifier = {
    // ${id} -> ${id}
    ConnectorPattern.findFirstMatchIn(identifier) match {
      case Some(matcher) => {
        if(matcher.groupCount != 2){
          throw new ParseException(s"Illegal connector definition: $identifier")
        }else{
          val source = matcher.group(1)
          val destination = matcher.group(2)
          if(source.contains(UnitFlagChar)) {
            val sources = source.split(UnitFlagSplitPattern).toSeq
            ConnectionIdentifier(sources.map{_.trim()},destination)
          }else{
            ConnectionIdentifier(Seq(source),destination)
          }
        }
      }
      case None => {
        if(identifier.contains(ConnectorFlag)) throw new ParseException(s"Failed to parse $identifier")
        DefinitionIdentifier(identifier)
      }
    }
  }
}

object DataFlow extends DataFlowParser