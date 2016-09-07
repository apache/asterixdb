/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.utils;

import java.util.ArrayList;

import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.configuration.Extension;
import org.apache.asterix.common.configuration.Property;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class ConfigUtil {

    private ConfigUtil() {
    }

    public static AsterixExtension toAsterixExtension(Extension ext) {
        String className = ext.getExtensionClassName();
        ArrayList<Pair<String, String>> args = new ArrayList<>();
        for (Property property : ext.getProperty()) {
            args.add(new Pair<>(property.getName(), property.getValue()));
        }
        return new AsterixExtension(className, args);
    }
}
