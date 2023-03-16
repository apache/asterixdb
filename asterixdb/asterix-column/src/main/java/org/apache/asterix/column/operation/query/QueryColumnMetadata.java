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
import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.reader.filter.FilterAccessorProvider;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluatorFactory;
import org.apache.asterix.column.values.reader.filter.IColumnFilterValueAccessor;
import org.apache.asterix.column.values.reader.filter.evaluator.FalseColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.evaluator.TrueColumnFilterEvaluator;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.DataProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Query column metadata which is used to resolve the requested values in a query
 */
public class QueryColumnMetadata extends AbstractColumnImmutableReadMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final FieldNamesDictionary fieldNamesDictionary;
    private final IColumnValuesReader[] primaryKeyReaders;
    private final IColumnFilterEvaluator filterEvaluator;
    private final List<IColumnFilterValueAccessor> filterValueAccessors;

    protected final ColumnAssembler assembler;

    protected QueryColumnMetadata(ARecordType datasetType, ARecordType metaType,
            IColumnValuesReader[] primaryKeyReaders, IValueReference serializedMetadata,
            FieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, IColumnValuesReaderFactory readerFactory,
            IValueGetterFactory valueGetterFactory, IColumnFilterEvaluator filterEvaluator,
            List<IColumnFilterValueAccessor> filterValueAccessors) throws HyracksDataException {
        super(datasetType, metaType, primaryKeyReaders.length, serializedMetadata, -1);
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.assembler = new ColumnAssembler(root, datasetType, this, readerFactory, valueGetterFactory);
        this.primaryKeyReaders = primaryKeyReaders;
        this.filterEvaluator = filterEvaluator;
        this.filterValueAccessors = filterValueAccessors;
    }

    public final ColumnAssembler getAssembler() {
        return assembler;
    }

    public final FieldNamesDictionary getFieldNamesDictionary() {
        return fieldNamesDictionary;
    }

    public final IColumnValuesReader[] getPrimaryKeyReaders() {
        return primaryKeyReaders;
    }

    public IColumnFilterEvaluator getFilterEvaluator() {
        return filterEvaluator;
    }

    public List<IColumnFilterValueAccessor> getFilterValueAccessors() {
        return filterValueAccessors;
    }

    /* *****************************************************
     * Non-final methods
     * *****************************************************
     */

    public boolean containsMeta() {
        return false;
    }

    @Override
    public int getColumnIndex(int ordinal) {
        return assembler.getColumnIndex(ordinal);
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return assembler.getNumberOfColumns();
    }

    @Override
    public int getNumberOfColumns() {
        return assembler.getNumberOfColumns();
    }

    @Override
    public AbstractColumnTupleReader createTupleReader() {
        return new QueryColumnTupleReader(this);
    }

    /**
     * Create {@link QueryColumnMetadata} that would be used to determine the requested values
     *
     * @param datasetType         dataset declared type
     * @param numberOfPrimaryKeys number of PKs
     * @param serializedMetadata  inferred metadata (schema)
     * @param readerFactory       column reader factory
     * @param valueGetterFactory  value serializer
     * @param requestedType       the requested schema
     * @return query metadata
     */
    public static QueryColumnMetadata create(ARecordType datasetType, int numberOfPrimaryKeys,
            IValueReference serializedMetadata, IColumnValuesReaderFactory readerFactory,
            IValueGetterFactory valueGetterFactory, ARecordType requestedType,
            Map<String, FunctionCallInformation> functionCallInfoMap,
            IColumnFilterEvaluatorFactory filterEvaluatorFactory, IWarningCollector warningCollector)
            throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int fieldNamesStart = offset + IntegerPointable.getInteger(bytes, offset + FIELD_NAMES_POINTER);
        int metaRootStart = IntegerPointable.getInteger(bytes, offset + META_SCHEMA_POINTER);
        int metaRootSize =
                metaRootStart < 0 ? 0 : IntegerPointable.getInteger(bytes, offset + PATH_INFO_POINTER) - metaRootStart;
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, length));

        //FieldNames
        FieldNamesDictionary fieldNamesDictionary = FieldNamesDictionary.deserialize(input);

        //Schema
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);
        //Skip metaRoot (if exists)
        input.skipBytes(metaRootSize);

        //Clip schema
        SchemaClipperVisitor clipperVisitor =
                new SchemaClipperVisitor(fieldNamesDictionary, functionCallInfoMap, warningCollector);
        ObjectSchemaNode clippedRoot = clip(requestedType, root, clipperVisitor);

        FilterAccessorProvider filterAccessorProvider = new FilterAccessorProvider(root, clipperVisitor);
        IColumnFilterEvaluator filterEvaluator = filterEvaluatorFactory.create(filterAccessorProvider);
        List<IColumnFilterValueAccessor> filterValueAccessors = filterAccessorProvider.getFilterAccessors();

        IColumnValuesReader[] primaryKeyReaders = createPrimaryKeyReaders(input, readerFactory, numberOfPrimaryKeys);

        if (LOGGER.isInfoEnabled() && filterEvaluator != TrueColumnFilterEvaluator.INSTANCE) {
            String filterString = filterEvaluator == FalseColumnFilterEvaluator.INSTANCE ? "SKIP_ALL"
                    : filterEvaluatorFactory.toString();
            LOGGER.info("Filter: {}", filterString);
        }

        return new QueryColumnMetadata(datasetType, null, primaryKeyReaders, serializedMetadata, fieldNamesDictionary,
                clippedRoot, readerFactory, valueGetterFactory, filterEvaluator, filterValueAccessors);
    }

    protected static ObjectSchemaNode clip(ARecordType requestedType, ObjectSchemaNode root,
            SchemaClipperVisitor clipperVisitor) {
        ObjectSchemaNode clippedRoot;
        if (requestedType.getTypeName().equals(DataProjectionFiltrationInfo.ALL_FIELDS_TYPE.getTypeName())) {
            clippedRoot = root;
        } else {
            clippedRoot = (ObjectSchemaNode) requestedType.accept(clipperVisitor, root);
        }
        return clippedRoot;
    }

    protected static IColumnValuesReader[] createPrimaryKeyReaders(DataInput input,
            IColumnValuesReaderFactory readerFactory, int numberOfPrimaryKeys) throws IOException {
        //skip number of columns
        input.readInt();

        IColumnValuesReader[] primaryKeyReaders = new IColumnValuesReader[numberOfPrimaryKeys];
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyReaders[i] = readerFactory.createValueReader(input);
        }
        return primaryKeyReaders;
    }
}
