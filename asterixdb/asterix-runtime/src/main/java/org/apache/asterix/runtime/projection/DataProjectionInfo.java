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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionInfo;

public class DataProjectionInfo implements IProjectionInfo<ARecordType> {
    //Default open record type when requesting the entire fields
    public static final ARecordType ALL_FIELDS_TYPE = createType("");
    //Default open record type when requesting none of the fields
    public static final ARecordType EMPTY_TYPE = createType("{}");

    private final ARecordType root;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;

    public DataProjectionInfo(ARecordType root, Map<String, FunctionCallInformation> sourceInformationMap) {
        this.root = root;
        this.functionCallInfoMap = sourceInformationMap;
    }

    private DataProjectionInfo(DataProjectionInfo other) {
        if (other.root == ALL_FIELDS_TYPE) {
            root = ALL_FIELDS_TYPE;
        } else if (other.root == EMPTY_TYPE) {
            root = EMPTY_TYPE;
        } else {
            root = other.root.deepCopy(other.root);
        }
        functionCallInfoMap = new HashMap<>(other.functionCallInfoMap);
    }

    @Override
    public ARecordType getProjectionInfo() {
        return root;
    }

    @Override
    public DataProjectionInfo createCopy() {
        return new DataProjectionInfo(this);
    }

    public Map<String, FunctionCallInformation> getFunctionCallInfoMap() {
        return functionCallInfoMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataProjectionInfo otherInfo = (DataProjectionInfo) o;
        return root.deepEqual(otherInfo.root) && Objects.equals(functionCallInfoMap, otherInfo.functionCallInfoMap);
    }

    @Override
    public String toString() {
        if (root == ALL_FIELDS_TYPE || root == EMPTY_TYPE) {
            //Return the type name if all fields or empty types
            return root.getTypeName();
        }
        //Return a oneliner JSON like representation for the requested fields
        StringBuilder builder = new StringBuilder();
        SimpleStringBuilderForIATypeVisitor visitor = new SimpleStringBuilderForIATypeVisitor();
        root.accept(visitor, builder);
        return builder.toString();
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

    private static ARecordType createType(String typeName) {
        return new ARecordType(typeName, new String[] {}, new IAType[] {}, true);
    }
}
