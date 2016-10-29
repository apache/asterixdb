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
package org.apache.asterix.common.config;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractAsterixProperties {
    private static final Logger LOGGER = Logger.getLogger(AbstractAsterixProperties.class.getName());
    private static final List<AbstractAsterixProperties> IMPLS = Collections.synchronizedList(new ArrayList<>());

    protected final AsterixPropertiesAccessor accessor;

    public AbstractAsterixProperties(AsterixPropertiesAccessor accessor) {
        this.accessor = accessor;
        IMPLS.add(this);
    }

    public Map<String, Object> getProperties() {
        return getProperties(UnaryOperator.identity());
    }

    public Map<String, Object> getProperties(UnaryOperator<String> keyTransformer) {
        Map<String, Object> properties = new HashMap<>();
        for (Method m : getClass().getMethods()) {
            PropertyKey key = m.getAnnotation(PropertyKey.class);
            if (key != null) {
                try {
                    properties.put(keyTransformer.apply(key.value()), m.invoke(this));
                } catch (Exception e) {
                    LOGGER.log(Level.INFO, "Error accessing property: " + key.value(), e);
                }
            }
        }
        return properties;
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface PropertyKey {
        String value();
    }

    public static List<AbstractAsterixProperties> getImplementations() {
        return Collections.unmodifiableList(IMPLS);
    }
}
