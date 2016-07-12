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
package org.apache.eagle.app.base.metadata;

import jdk.nashorn.internal.ir.annotations.Immutable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "configuration")
@XmlAccessorType(XmlAccessType.FIELD)
@Immutable
public class ConfigurationSpec {
    @XmlElement(name = "property")
    private List<PropertySpec> properties;

    public List<PropertySpec> getProperties() {
        return properties;
    }

    public PropertySpec getProperty(String name){
        for(PropertySpec propertySpec:properties){
            if(propertySpec.getName().equals(name)){
                return propertySpec;
            }
        }
        return null;
    }

    public boolean hasProperty(String name){
        for(PropertySpec propertySpec:properties){
            if(propertySpec.getName().equals(name)){
                return true;
            }
        }
        return false;
    }
}