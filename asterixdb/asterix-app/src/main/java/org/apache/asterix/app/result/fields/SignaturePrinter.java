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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.annotations.IRecordTypeAnnotation;
import org.apache.asterix.common.annotations.RecordFieldOrderAnnotation;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.ResultMetadata;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SignaturePrinter implements IResponseFieldPrinter {

    public static final SignaturePrinter INSTANCE = new SignaturePrinter(null);
    private static final String FIELD_NAME = "signature";
    private static final String NAME_FIELD_NAME = "name";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String OPTIONAL_MODIFIER = "?";

    private final String signature;

    public SignaturePrinter(String signature) {
        this.signature = signature;
    }

    @Override
    public void print(PrintWriter pw) {
        pw.print("\t\"");
        pw.print(FIELD_NAME);
        pw.print("\": ");
        if (signature == null) {
            pw.print("{\n\t");
            ResultUtil.printField(pw, "*", "*", false);
            pw.print("\n\t}");
        } else {
            pw.print(signature);
        }
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }

    public static String generateFlatSignature(ResultMetadata resultMetadata) {
        List<Object> typeInfo = resultMetadata.getOutputTypes();
        if (typeInfo == null || typeInfo.size() != 1) {
            return null;
        }
        IAType outputType = TypeComputeUtils.getActualType((IAType) typeInfo.get(0));
        if (outputType.getTypeTag() != ATypeTag.OBJECT) {
            return null;
        }
        ARecordType outputRecordType = (ARecordType) outputType;
        String[] fieldNames;
        IAType[] fieldTypes;
        Pair<String[], IAType[]> p = generateFlatSignatureFromOpenType(outputRecordType);
        if (p != null) {
            fieldNames = p.first;
            fieldTypes = p.second;
        } else {
            fieldNames = outputRecordType.getFieldNames();
            fieldTypes = outputRecordType.getFieldTypes();
        }
        if (fieldNames == null || fieldNames.length == 0) {
            return null;
        }
        ObjectNode signatureNode = JSONUtil.createObject();
        ArrayNode fieldNameArrayNode = signatureNode.putArray(NAME_FIELD_NAME);
        ArrayNode fieldTypeArrayNode = signatureNode.putArray(TYPE_FIELD_NAME);
        for (int i = 0, n = fieldNames.length; i < n; i++) {
            fieldNameArrayNode.add(fieldNames[i]);
            fieldTypeArrayNode.add(printFieldType(fieldTypes[i]));
        }
        return signatureNode.toString();
    }

    private static Pair<String[], IAType[]> generateFlatSignatureFromOpenType(ARecordType outputRecordType) {
        if (!outputRecordType.isOpen() || !outputRecordType.knowsAllPossibleAdditonalFieldNames()) {
            return null;
        }
        IRecordTypeAnnotation fieldOrderAnn =
                outputRecordType.findAnnotation(IRecordTypeAnnotation.Kind.RECORD_FIELD_ORDER);
        if (fieldOrderAnn == null) {
            return null;
        }
        LinkedHashSet<String> fieldNamesOrdered = ((RecordFieldOrderAnnotation) fieldOrderAnn).getFieldNames();
        int numFields = fieldNamesOrdered.size();

        String[] fieldNames = new String[numFields];
        IAType[] fieldTypes = new IAType[numFields];
        int i = 0;
        for (String fieldName : fieldNamesOrdered) {
            IAType fieldType;
            if (outputRecordType.isClosedField(fieldName)) {
                fieldType = outputRecordType.getFieldType(fieldName);
            } else if (outputRecordType.getAllPossibleAdditonalFieldNames().contains(fieldName)) {
                fieldType = BuiltinType.ANY;
            } else {
                return null;
            }
            fieldNames[i] = fieldName;
            fieldTypes[i] = fieldType;
            i++;
        }
        return new Pair<>(fieldNames, fieldTypes);
    }

    private static String printFieldType(IAType type) {
        boolean optional = false;
        if (type.getTypeTag() == ATypeTag.UNION) {
            AUnionType fieldTypeUnion = (AUnionType) type;
            optional = fieldTypeUnion.isNullableType() || fieldTypeUnion.isMissableType();
            type = fieldTypeUnion.getActualType();
        }
        String typeName;
        switch (type.getTypeTag()) {
            case OBJECT:
            case ARRAY:
            case MULTISET:
                typeName = type.getDisplayName();
                break;
            default:
                typeName = type.getTypeName();
                break;
        }
        return optional ? typeName + OPTIONAL_MODIFIER : typeName;
    }

    public static SignaturePrinter newInstance(ExecutionPlans executionPlans) {
        String signature = executionPlans.getSignature();
        return signature != null ? new SignaturePrinter(signature) : INSTANCE;
    }
}
