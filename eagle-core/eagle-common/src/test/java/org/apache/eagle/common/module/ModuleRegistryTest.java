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

package org.apache.eagle.common.module;

import com.google.inject.AbstractModule;
import org.junit.Assert;
import org.junit.Test;

public class ModuleRegistryTest {
    @Test
    public void testPutAndGet() {
        ModuleRegistry registry = new ModuleRegistryImpl();
        registry.register(TestModuleScope1.class, new AbstractModule() {
            @Override
            protected void configure() {

            }
        });
        registry.register(TestModuleScope2.class, new AbstractModule() {
            @Override
            protected void configure() {

            }
        });
        registry.register(TestModuleScope1.class, new AbstractModule() {
            @Override
            protected void configure() {

            }
        });
        Assert.assertEquals(2, registry.getModules(TestModuleScope1.class).size());
        Assert.assertEquals(1, registry.getModules(TestModuleScope2.class).size());
        Assert.assertEquals(3, registry.getModules().size());
    }

    private class TestModuleScope1 implements ModuleScope {
    }

    private class TestModuleScope2 implements ModuleScope {
    }
}
