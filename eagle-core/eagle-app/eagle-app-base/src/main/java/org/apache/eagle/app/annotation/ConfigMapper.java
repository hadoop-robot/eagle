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
package org.apache.eagle.app.annotation;

import com.typesafe.config.Config;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.eagle.app.Configuration;
import org.apache.eagle.app.exception.IllegalConfigurationException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ConfigMapper<T extends Configuration> {
    private final ConfigMeta<T> meta;
    public ConfigMapper(Class<T> configurationClass){
        this.meta = new ConfigMeta<>(configurationClass);
    }

    public void mapTo(Config config, Object configuration){
        meta.getFieldProperties().forEach((field,property)->{
            String path = property.value() == null ? property.name(): property.value();
            if(path == null) path = field.getName();
            if(config.hasPath(path) || property.required()){
                try {
                    PropertyUtils.setProperty(configuration,field.getName(), config.getValue(path));
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new IllegalConfigurationException(e.getMessage(),e);
                }
            }
        });
    }

    public T mapFrom(Config config){
        try {
            T configuration = this.meta.newInstance();
            configuration.load(config);
            mapTo(config,configuration);
            return configuration;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(),e);
        }
    }

    private final static Map<Class<? extends Configuration>,ConfigMapper<?>> CACHE = new HashMap<>();
    public static <T extends Configuration> ConfigMapper<T> cached(Class<T> clazz){
        synchronized (ConfigMapper.class) {
            if (!CACHE.containsKey(clazz)) {
                ConfigMapper<T> mapper = new ConfigMapper<>(clazz);
                CACHE.put(clazz, mapper);
                return mapper;
            } else {
                return (ConfigMapper<T>) CACHE.get(clazz);
            }
        }
    }
}