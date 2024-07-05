
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
package org.apache.asterix.runtime.aggregates.std;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.api.IRowWriteMultiPageOp;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.UnsafeUtil;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.RowSchemaTransformer;
import org.apache.asterix.runtime.schemainferrence.RowTransformer;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.RowFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;


public abstract class AbstractSchemaAggregateFunction extends AbstractAggregateFunction {

    private final IEvaluatorContext context;

    // Warning flag to warn only once in case of non-numeric data
    private boolean isWarned;

    private final ARecordType recType;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private RecordLazyVisitablePointable inputVal;
    private IScalarEvaluator eval;
    protected ATypeTag aggType;
    private RowTransformer transformer;
    private RowSchemaTransformer schemaTransformer;
    RowMetadata rowMetaData;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

    public AbstractSchemaAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType aggFieldState) throws HyracksDataException {
        super(sourceLoc);
        this.context = context;
        eval = args[0].createScalarEvaluator(context);
        recType = (ARecordType) aggFieldState;
        inputVal = new TypedRecordLazyVisitablePointable(recType);
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        isWarned = false;
        Mutable<IRowWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        rowMetaData = new RowMetadata(multiPageOpRef);
        transformer = new RowTransformer(rowMetaData, rowMetaData.getRoot());
        schemaTransformer = new RowSchemaTransformer(rowMetaData, rowMetaData.getRoot());

    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected abstract void processNull();

    // TODO : CALVIN DANI
    protected void processDataValues(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        int recordFieldId = rowMetaData.getRecordFieldIndex();
        inputVal.set(tuple.getFieldData(recordFieldId), tuple.getFieldStart(recordFieldId),
                tuple.getFieldLength(recordFieldId));
        transformer.transform(inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        ATypeTag aggTypeTag = aggType;
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull();
            return;
        } else if (aggTypeTag == ATypeTag.SYSTEM_NULL) {
            aggTypeTag = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggTypeTag)) {
            // Issue warning only once and treat current tuple as null
            if (!isWarned) {
                isWarned = true;
                ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
            }
            processNull();
            return;
        } else if (ATypeHierarchy.canPromote(aggTypeTag, typeTag)) {
            aggTypeTag = typeTag;
        }
        aggType = aggTypeTag;

    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {

        resultStorage.reset();
        try {
            if (rowMetaData != null) {
                IValueReference serSchema = rowMetaData.serializeColumnsMetadata();
                result.set(serSchema);
            } // Double check that count 0 is accounted
            else if (aggType == ATypeTag.SYSTEM_NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else if (aggType == ATypeTag.NULL) {
                // TODO : CALVIN DANI Check size of serialization of data
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                result.set(resultStorage);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }

        byte[] serBytes = tuple.getFrameTupleAccessor().getBuffer().array();

        int offset = 9; // TODO (CALVIN DANI) Look at written offset for storing fieldnames TO CHANGE
        int fieldNamesStart = offset + UnsafeUtil.getInt(serBytes, offset + 4);
        int metaRootStart = UnsafeUtil.getInt(serBytes, offset + 12);
        int length = UnsafeUtil.getInt(serBytes, offset + 20);
        int metaRootSize = metaRootStart < 0 ? 0 : UnsafeUtil.getInt(serBytes, offset + 16) - metaRootStart;
        DataInput input = new DataInputStream(new ByteArrayInputStream(serBytes, fieldNamesStart, length));

        try {
            //FieldNames //TODO CALVIN DANI check fieldNamesDictionary if it is required
            RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
            //Schema
            ObjectRowSchemaNode root = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input);
            schemaTransformer.transform(root);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            ObjectRowSchemaNode root = schemaTransformer.getRoot();
            String res = rowMetaData.printRootSchema(root, rowMetaData.getFieldNamesDictionary());
            if (root == null) {
                throw new HyracksDataException("Cannot compute Schema on empty root.");
            } else {
                stringSerde.serialize(new AString(res), resultStorage.getDataOutput());
            }

        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }

    // Function identifier
    private FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SQL_SCHEMA;
    }
}
