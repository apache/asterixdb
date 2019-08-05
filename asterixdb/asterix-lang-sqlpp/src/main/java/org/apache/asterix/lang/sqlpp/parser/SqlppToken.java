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

package org.apache.asterix.lang.sqlpp.parser;

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class SqlppToken implements Serializable {

    public SourceLocation sourceLocation;

    public SqlppHint hint;
    public String hintParams;

    public boolean parseHint(String text) {
        int paramStart = SqlppHint.findParamStart(text);
        String id = paramStart >= 0 ? text.substring(0, paramStart) : text;
        hint = SqlppHint.findByIdentifier(id);
        if (hint != null) {
            hintParams = paramStart >= 0 ? text.substring(paramStart).trim() : null;
            return true;
        } else {
            hintParams = text;
            return false;
        }
    }
}
