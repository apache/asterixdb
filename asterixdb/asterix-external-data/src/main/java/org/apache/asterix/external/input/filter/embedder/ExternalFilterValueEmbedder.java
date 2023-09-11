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
package org.apache.asterix.external.input.filter.embedder;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;

public class ExternalFilterValueEmbedder implements IExternalFilterValueEmbedder {
    private final ARecordType allPaths;
    private final IWarningCollector warningCollector;
    private final Map<IAType, BitSet> setValues;
    private final EmbeddedValueBuilder builder;
    private final Stack<ARecordType> parents;
    private final int numberOfValues;
    private int level;
    private int numberOfSetValues;
    private IAType currentType;
    private BitSet currentSetValues;

    public ExternalFilterValueEmbedder(ARecordType allPaths, List<ProjectionFiltrationTypeUtil.RenamedType> leafs,
            ExternalDataPrefix prefix, IWarningCollector warningCollector) {
        this.allPaths = allPaths;
        this.warningCollector = warningCollector;
        setValues = new HashMap<>();
        builder = new EmbeddedValueBuilder(allPaths, prefix, setValues);
        parents = new Stack<>();
        numberOfValues = leafs.size();
        reset();
    }

    @Override
    public void reset() {
        currentType = allPaths;
        numberOfSetValues = 0;
        currentSetValues = setValues.get(allPaths);
        currentSetValues.clear();
        parents.clear();
        parents.push(allPaths);
        level = 0;
    }

    @Override
    public void setPath(String path) throws HyracksDataException {
        builder.build(path);
    }

    @Override
    public boolean shouldEmbed(String fieldName, ATypeTag typeTag) {
        return shouldEmbed(fieldName, typeTag, false);
    }

    @Override
    public IValueReference getEmbeddedValue() {
        IValueReference value = builder.getValue(currentType);
        if (currentType.getTypeTag() != ATypeTag.OBJECT) {
            numberOfSetValues++;
            currentType = parents.peek();
        } else if (currentType != allPaths) {
            currentType = parents.pop();
        }
        return value;
    }

    @Override
    public boolean isMissingEmbeddedValues() {
        if (currentType.getTypeTag() == ATypeTag.MISSING) {
            return false;
        }
        int unsetValue = currentSetValues.nextClearBit(0);
        int numberOfFields = ((ARecordType) currentType).getFieldTypes().length;
        return unsetValue <= numberOfFields - 1;
    }

    @Override
    public boolean isMissing(String fieldName) {
        ARecordType parent = (ARecordType) currentType;
        return !currentSetValues.get(parent.getFieldIndex(fieldName)) && shouldEmbed(fieldName, ATypeTag.ANY, true);
    }

    @Override
    public String[] getEmbeddedFieldNames() {
        return ((ARecordType) currentType).getFieldNames();
    }

    @Override
    public void enterObject() {
        level++;
        if (level != parents.size()) {
            parents.push((ARecordType) currentType);
            currentType = BuiltinType.AMISSING;
        }
    }

    @Override
    public void exitObject() {
        if (currentType != allPaths) {
            currentType = parents.pop();
            currentSetValues = setValues.get(currentType);
        }
        level--;
    }

    private boolean shouldEmbed(String fieldName, ATypeTag typeTag, boolean includeObjects) {
        if (currentType.getTypeTag() == ATypeTag.MISSING || numberOfSetValues == numberOfValues) {
            return false;
        }

        ARecordType recordType = (ARecordType) currentType;
        int index = recordType.getFieldIndex(fieldName);

        if (index < 0 || currentSetValues.get(index)) {
            return false;
        }

        currentSetValues.set(index);
        IAType fieldType = recordType.getFieldTypes()[index];
        if (fieldType.getTypeTag() == ATypeTag.OBJECT) {
            ARecordType newParent = (ARecordType) fieldType;
            parents.push(recordType);
            currentType = fieldType;
            currentSetValues = setValues.get(newParent);
            currentSetValues.clear();
            return includeObjects || typeTag != ATypeTag.OBJECT;
        } else {
            currentType = fieldType;
            return true;
        }
    }
}
