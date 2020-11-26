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
package org.apache.asterix.metadata.declared;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class ExternalDataProjectionInfo implements IProjectionInfo<List<String>> {
    private final List<List<String>> projectedFieldNames;

    public ExternalDataProjectionInfo() {
        projectedFieldNames = new ArrayList<>();
    }

    private ExternalDataProjectionInfo(List<List<String>> projectedFieldNames) {
        this.projectedFieldNames = new ArrayList<>();
        for (List<String> path : projectedFieldNames) {
            List<String> newPath = new ArrayList<>(path);
            this.projectedFieldNames.add(newPath);
        }
    }

    @Override
    public List<List<String>> getProjectionInfo() {
        return projectedFieldNames;
    }

    @Override
    public IProjectionInfo<List<String>> createCopy() {
        return new ExternalDataProjectionInfo(projectedFieldNames);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ExternalDataProjectionInfo)) {
            return false;
        }
        List<List<String>> otherProjectedFieldNames = ((ExternalDataProjectionInfo) other).projectedFieldNames;
        return projectedFieldNames.size() == otherProjectedFieldNames.size()
                && VariableUtilities.varListEqualUnordered(projectedFieldNames, otherProjectedFieldNames);
    }

    public String toString() {
        if (projectedFieldNames.isEmpty()) {
            return "";
        }
        final StringBuilder fieldNamesBuilder = new StringBuilder();
        append(fieldNamesBuilder);
        return fieldNamesBuilder.toString();
    }

    /**
     * Append projected field names to the external dataset properties
     */
    public void addToProperties(Map<String, String> properties) {
        final String pushedFieldNames = toString();
        if (!pushedFieldNames.isEmpty()) {
            properties.put(ExternalDataConstants.KEY_REQUESTED_FIELDS, toString());
        }
    }

    private void append(StringBuilder builder) {
        appendFieldNames(projectedFieldNames.get(0), builder);
        for (int i = 1; i < projectedFieldNames.size(); i++) {
            builder.append(", ");
            appendFieldNames(projectedFieldNames.get(i), builder);
        }
    }

    private void appendFieldNames(List<String> fieldNames, StringBuilder builder) {
        builder.append(fieldNames.get(0));
        for (int i = 1; i < fieldNames.size(); i++) {
            builder.append('.').append(fieldNames.get(i));
        }
    }

}
