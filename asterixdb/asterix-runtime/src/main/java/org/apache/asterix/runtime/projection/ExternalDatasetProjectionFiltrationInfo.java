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
package org.apache.asterix.runtime.projection;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksStringBuilderWriter;

import com.fasterxml.jackson.core.JsonGenerator;

public class ExternalDatasetProjectionFiltrationInfo implements IProjectionFiltrationInfo {
    protected final ARecordType projectedType;

    protected final ILogicalExpression filterExpression;
    protected final Map<ILogicalExpression, ARecordType> filterPaths;
    protected final Map<String, FunctionCallInformation> functionCallInfoMap;

    public ExternalDatasetProjectionFiltrationInfo(ARecordType projectedType,
            Map<String, FunctionCallInformation> sourceInformationMap, Map<ILogicalExpression, ARecordType> filterPaths,
            ILogicalExpression filterExpression) {
        this.projectedType = projectedType;
        this.functionCallInfoMap = sourceInformationMap;
        this.filterExpression = filterExpression;
        this.filterPaths = filterPaths;
    }

    private ExternalDatasetProjectionFiltrationInfo(ExternalDatasetProjectionFiltrationInfo other) {
        if (other.projectedType == ALL_FIELDS_TYPE) {
            projectedType = ALL_FIELDS_TYPE;
        } else if (other.projectedType == EMPTY_TYPE) {
            projectedType = EMPTY_TYPE;
        } else {
            projectedType = other.projectedType.deepCopy(other.projectedType);
        }
        functionCallInfoMap = new HashMap<>(other.functionCallInfoMap);

        filterExpression = other.filterExpression;
        filterPaths = new HashMap<>(other.filterPaths);
    }

    @Override
    public ExternalDatasetProjectionFiltrationInfo createCopy() {
        return new ExternalDatasetProjectionFiltrationInfo(this);
    }

    public ARecordType getProjectedType() {
        return projectedType;
    }

    public Map<String, FunctionCallInformation> getFunctionCallInfoMap() {
        return functionCallInfoMap;
    }

    public ILogicalExpression getFilterExpression() {
        return filterExpression;
    }

    public Map<ILogicalExpression, ARecordType> getFilterPaths() {
        return filterPaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalDatasetProjectionFiltrationInfo otherInfo = (ExternalDatasetProjectionFiltrationInfo) o;
        return projectedType.deepEqual(otherInfo.projectedType)
                && Objects.equals(functionCallInfoMap, otherInfo.functionCallInfoMap);
    }

    @Override
    public void print(AlgebricksStringBuilderWriter writer) {
        if (projectedType != ALL_FIELDS_TYPE) {
            writer.append(" project (");
            if (projectedType == EMPTY_TYPE) {
                writer.append(projectedType.getTypeName());
            } else {
                writer.append(getOnelinerSchema(projectedType, new StringBuilder()));
            }
            writer.append(')');
        }

        if (filterExpression != null) {
            writer.append(" prefix-filter on: ");
            writer.append(filterExpression.toString());
        }
    }

    @Override
    public void print(JsonGenerator generator) throws IOException {
        StringBuilder builder = new StringBuilder();
        if (projectedType != ALL_FIELDS_TYPE) {
            if (projectedType == EMPTY_TYPE) {
                generator.writeStringField("project", projectedType.getTypeName());
            } else {
                generator.writeStringField("project", getOnelinerSchema(projectedType, builder));
            }
        }

        if (filterExpression != null) {
            generator.writeStringField("prefix-filter-on", filterExpression.toString());
        }
    }

    protected String getOnelinerSchema(ARecordType type, StringBuilder builder) {
        //Return oneliner JSON like representation for the requested fields
        SimpleStringBuilderForIATypeVisitor visitor = new SimpleStringBuilderForIATypeVisitor();
        type.accept(visitor, builder);
        String onelinerSchema = builder.toString();
        builder.setLength(0);
        return onelinerSchema;
    }

    /**
     * Serialize expected record type
     *
     * @param expectedRecordType expected record type
     * @param output             data output
     */
    public static void writeTypeField(ARecordType expectedRecordType, DataOutput output) throws IOException {
        byte[] recordTypeBytes = SerializationUtils.serialize(expectedRecordType);
        output.writeInt(recordTypeBytes.length);
        output.write(recordTypeBytes);
    }

    /**
     * Deserialize expected record type
     *
     * @param input data input
     * @return deserialized expected record type
     */
    public static ARecordType createTypeField(DataInput input) throws IOException {
        int length = input.readInt();
        byte[] recordTypeBytes = new byte[length];
        input.readFully(recordTypeBytes, 0, length);
        return SerializationUtils.deserialize(recordTypeBytes);
    }

    /**
     * Serialize function call information map
     *
     * @param functionCallInfoMap function information map
     * @param output              data output
     */
    public static void writeFunctionCallInformationMapField(Map<String, FunctionCallInformation> functionCallInfoMap,
            DataOutput output) throws IOException {
        output.writeInt(functionCallInfoMap.size());
        for (Map.Entry<String, FunctionCallInformation> info : functionCallInfoMap.entrySet()) {
            output.writeUTF(info.getKey());
            info.getValue().writeFields(output);
        }
    }

    /**
     * Deserialize function call information map
     *
     * @param input data input
     * @return deserialized function call information map
     */
    public static Map<String, FunctionCallInformation> createFunctionCallInformationMap(DataInput input)
            throws IOException {
        int size = input.readInt();
        Map<String, FunctionCallInformation> functionCallInfoMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = input.readUTF();
            FunctionCallInformation functionCallInfo = FunctionCallInformation.create(input);
            functionCallInfoMap.put(key, functionCallInfo);
        }
        return functionCallInfoMap;
    }
}
