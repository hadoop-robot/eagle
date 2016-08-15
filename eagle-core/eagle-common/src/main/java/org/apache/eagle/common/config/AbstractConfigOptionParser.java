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

package org.apache.eagle.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import java.util.Map;

public abstract class AbstractConfigOptionParser {

    // private final Options options;
    private final Parser parser;

    public AbstractConfigOptionParser() {
        parser = parser();
        //options = options();
    }

    /**
     * Get current common-cli Parser instance.
     *
     * @return Parser
     */
    protected abstract Parser parser();

    /**
     * Get current options.
     *
     * @return Options
     */
    protected abstract Options options();

    public abstract Map<String, String> parseConfig(String[] arguments) throws ParseException;

    /**
     * Load config as system properties.
     *
     * @param arguments command line arguments
     * @throws ParseException parsing exception for CLI arguments
     */
    public Config load(String[] arguments) throws ParseException {
        Map<String, String> configProps = parseConfig(arguments);
        for (Map.Entry<String, String> entry : configProps.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }
        System.setProperty("config.trace", "loads");
        return ConfigFactory.load();
    }

    public CommandLine parse(String[] arguments) throws ParseException {
        return this.parser.parse(this.options(), arguments);
    }
}