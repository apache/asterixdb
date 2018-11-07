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
package org.apache.asterix.lang.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;

public class ConfigurationUtil {

    private ConfigurationUtil() {
    }

    /**
     * Convert the parameters object to a Map<String,String>
     * This method should go away once we store the with object as it is in storage
     *
     * @param parameters
     *            the parameters passed for the merge policy in the with clause
     * @return the parameters as a map
     */
    public static Map<String, String> toProperties(AdmObjectNode parameters) throws CompilationException {
        Map<String, String> map = new HashMap<>();
        for (Entry<String, IAdmNode> field : parameters.getFields()) {
            IAdmNode value = field.getValue();
            map.put(field.getKey(), getStringValue(value));
        }
        return map;
    }

    /**
     * Get string value of {@link IAdmNode}
     *
     * @param value
     *            IAdmNode value should be of type integer or string
     * @return
     *         string value of <code>value</code>
     * @throws CompilationException
     */
    public static String getStringValue(IAdmNode value) throws CompilationException {
        switch (value.getType()) {
            case BOOLEAN:
            case DOUBLE:
            case BIGINT:
                return value.toString();
            case STRING:
                return ((AdmStringNode) value).get();
            default:
                throw new CompilationException(ErrorCode.CONFIGURATION_PARAMETER_INVALID_TYPE, value.getType());
        }
    }
}
