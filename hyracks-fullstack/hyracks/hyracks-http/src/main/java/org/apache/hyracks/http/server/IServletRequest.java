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
package org.apache.hyracks.http.server;

import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.FullHttpRequest;

/**
 * An Http Request instance
 */
public interface IServletRequest {
    /**
     * @return the full http request
     */
    FullHttpRequest getHttpRequest();

    /**
     * Get a request parameter
     * @param name
     * @return the parameter or null if not found
     */
    String getParameter(CharSequence name);

    /**
     * Get a request header
     * @param name
     * @return the header or null if not found
     */
    String getHeader(CharSequence name);

    static String getParameter(Map<String, List<String>> parameters, CharSequence name) {
        List<String> parameter = parameters.get(name);
        if (parameter == null) {
            return null;
        } else if (parameter.size() == 1) {
            return parameter.get(0);
        } else {
            StringBuilder aString = new StringBuilder(parameter.get(0));
            for (int i = 1; i < parameter.size(); i++) {
                aString.append(",").append(parameter.get(i));
            }
            return aString.toString();
        }
    }
}
