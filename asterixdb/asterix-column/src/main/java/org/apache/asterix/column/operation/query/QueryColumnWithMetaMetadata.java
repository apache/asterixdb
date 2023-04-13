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
package org.apache.asterix.column.operation.query;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.IValueGetterFactory;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.asterix.column.values.reader.filter.FilterAccessorProvider;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluatorFactory;
import org.apache.asterix.column.values.reader.filter.IColumnFilterValueAccessor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;

/**
 * Query column metadata (with metaRecord)
 */
public final class QueryColumnWithMetaMetadata extends QueryColumnMetadata {
    private final ColumnAssembler metaAssembler;

    private QueryColumnWithMetaMetadata(ARecordType datasetType, ARecordType metaType,
            PrimitiveColumnValuesReader[] primaryKeyReaders, IValueReference serializedMetadata,
            FieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, ObjectSchemaNode metaRoot,
            IColumnValuesReaderFactory readerFactory, IValueGetterFactory valueGetterFactory,
            IColumnFilterEvaluator filterEvaluator, List<IColumnFilterValueAccessor> filterValueAccessors)
            throws HyracksDataException {
        super(datasetType, metaType, primaryKeyReaders, serializedMetadata, fieldNamesDictionary, root, readerFactory,
                valueGetterFactory, filterEvaluator, filterValueAccessors);
        metaAssembler = new ColumnAssembler(metaRoot, metaType, this, readerFactory, valueGetterFactory);
    }

    @Override
    public boolean containsMeta() {
        return true;
    }

    @Override
    public int getColumnIndex(int ordinal) {
        int metaColumnCount = metaAssembler.getNumberOfColumns();
        if (ordinal >= metaColumnCount) {
            return assembler.getColumnIndex(ordinal - metaColumnCount);
        }
        return metaAssembler.getColumnIndex(ordinal);
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return metaAssembler.getNumberOfColumns() + assembler.getNumberOfColumns();
    }

    @Override
    public int getNumberOfColumns() {
        return getNumberOfProjectedColumns();
    }

    @Override
    public AbstractColumnTupleReader createTupleReader() {
        return new QueryColumnTupleReader(this);
    }

    public ColumnAssembler getMetaAssembler() {
        return metaAssembler;
    }

    /**
     * Create {@link QueryColumnWithMetaMetadata} that would be used to determine the requested values
     *
     * @param datasetType         dataset declared type
     * @param metaType            meta declared type
     * @param numberOfPrimaryKeys number of PKs
     * @param serializedMetadata  inferred metadata (schema)
     * @param readerFactory       column reader factory
     * @param valueGetterFactory  value serializer
     * @param requestedType       the requested schema
     * @return query metadata
     */
    public static QueryColumnWithMetaMetadata create(ARecordType datasetType, ARecordType metaType,
            int numberOfPrimaryKeys, IValueReference serializedMetadata, IColumnValuesReaderFactory readerFactory,
            IValueGetterFactory valueGetterFactory, ARecordType requestedType,
            Map<String, FunctionCallInformation> functionCallInfo, ARecordType metaRequestedType,
            Map<String, FunctionCallInformation> metaFunctionCallInfo,
            IColumnFilterEvaluatorFactory filterEvaluatorFactory, IWarningCollector warningCollector)
            throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int fieldNamesStart = offset + IntegerPointable.getInteger(bytes, offset + FIELD_NAMES_POINTER);
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, length));

        //FieldNames
        FieldNamesDictionary fieldNamesDictionary = FieldNamesDictionary.deserialize(input);

        //Schema
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);
        ObjectSchemaNode metaRoot = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);

        //Clip dataset schema
        SchemaClipperVisitor clipperVisitor =
                new SchemaClipperVisitor(fieldNamesDictionary, functionCallInfo, warningCollector);
        ObjectSchemaNode clippedRoot = clip(requestedType, root, clipperVisitor);

        //Clip meta schema
        SchemaClipperVisitor metaClipperVisitor =
                new SchemaClipperVisitor(fieldNamesDictionary, metaFunctionCallInfo, warningCollector);
        ObjectSchemaNode metaClippedRoot = clip(metaRequestedType, metaRoot, metaClipperVisitor);

        FilterAccessorProvider filterAccessorProvider = new FilterAccessorProvider(root, metaRoot, clipperVisitor);
        IColumnFilterEvaluator filterEvaluator = filterEvaluatorFactory.create(filterAccessorProvider);
        List<IColumnFilterValueAccessor> filterValueAccessors = filterAccessorProvider.getFilterAccessors();

        PrimitiveColumnValuesReader[] primaryKeyReaders =
                createPrimaryKeyReaders(input, readerFactory, numberOfPrimaryKeys);

        return new QueryColumnWithMetaMetadata(datasetType, metaType, primaryKeyReaders, serializedMetadata,
                fieldNamesDictionary, clippedRoot, metaClippedRoot, readerFactory, valueGetterFactory, filterEvaluator,
                filterValueAccessors);
    }
}
