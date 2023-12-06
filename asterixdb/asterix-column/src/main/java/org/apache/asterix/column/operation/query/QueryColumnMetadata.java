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

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.IValueGetterFactory;
import org.apache.asterix.column.filter.FalseColumnFilterEvaluator;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Query column metadata which is used to resolve the requested values in a query
 */
public class QueryColumnMetadata extends AbstractColumnImmutableReadMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final FieldNamesDictionary fieldNamesDictionary;
    private final PrimitiveColumnValuesReader[] primaryKeyReaders;
    private final IColumnFilterEvaluator normalizedFilterEvaluator;
    private final List<IColumnRangeFilterValueAccessor> filterValueAccessors;
    private final IColumnIterableFilterEvaluator columnFilterEvaluator;
    private final List<IColumnValuesReader> filterColumnReaders;

    protected final ColumnAssembler assembler;

    protected QueryColumnMetadata(ARecordType datasetType, ARecordType metaType,
            PrimitiveColumnValuesReader[] primaryKeyReaders, IValueReference serializedMetadata,
            FieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, IColumnValuesReaderFactory readerFactory,
            IValueGetterFactory valueGetterFactory, IColumnFilterEvaluator normalizedFilterEvaluator,
            List<IColumnRangeFilterValueAccessor> filterValueAccessors,
            IColumnIterableFilterEvaluator columnFilterEvaluator, List<IColumnValuesReader> filterColumnReaders)
            throws HyracksDataException {
        super(datasetType, metaType, primaryKeyReaders.length, serializedMetadata, -1);
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.primaryKeyReaders = primaryKeyReaders;
        this.normalizedFilterEvaluator = normalizedFilterEvaluator;
        this.filterValueAccessors = filterValueAccessors;
        this.columnFilterEvaluator = columnFilterEvaluator;
        this.filterColumnReaders = filterColumnReaders;
        // Must be the last value to set as it depends on 'QueryColumnMetadata.this'
        this.assembler = new ColumnAssembler(root, datasetType, this, readerFactory, valueGetterFactory);
    }

    public final ColumnAssembler getAssembler() {
        return assembler;
    }

    public final FieldNamesDictionary getFieldNamesDictionary() {
        return fieldNamesDictionary;
    }

    public final PrimitiveColumnValuesReader[] getPrimaryKeyReaders() {
        return primaryKeyReaders;
    }

    public final IColumnFilterEvaluator getRangeFilterEvaluator() {
        return normalizedFilterEvaluator;
    }

    public final List<IColumnRangeFilterValueAccessor> getFilterValueAccessors() {
        return filterValueAccessors;
    }

    public final IColumnIterableFilterEvaluator getColumnFilterEvaluator() {
        return columnFilterEvaluator;
    }

    public final List<IColumnValuesReader> getFilterColumnReaders() {
        return filterColumnReaders;
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
    public int getFilteredColumnIndex(int ordinal) {
        return filterColumnReaders.get(ordinal).getColumnIndex();
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return assembler.getNumberOfColumns();
    }

    @Override
    public int getNumberOfFilteredColumns() {
        return filterColumnReaders.size();
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
            IColumnRangeFilterEvaluatorFactory normalizedEvaluatorFactory,
            IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory, IWarningCollector warningCollector,
            IHyracksTaskContext context) throws IOException {
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

        IColumnFilterEvaluator normalizedFilterEvaluator = TrueColumnFilterEvaluator.INSTANCE;
        IColumnIterableFilterEvaluator columnFilterEvaluator = TrueColumnFilterEvaluator.INSTANCE;
        List<IColumnValuesReader> filterColumnReaders = Collections.emptyList();
        List<IColumnRangeFilterValueAccessor> filterValueAccessors = Collections.emptyList();
        String jobId = null;
        if (context != null) {
            FilterAccessorProvider accessorProvider =
                    new FilterAccessorProvider(root, clipperVisitor, readerFactory, valueGetterFactory);
            // Min/Max filters in page0
            normalizedFilterEvaluator = normalizedEvaluatorFactory.create(accessorProvider);
            filterValueAccessors = accessorProvider.getFilterAccessors();
            // Filter columns (columns appeared in WHERE clause)
            ColumnFilterEvaluatorContext evaluatorContext = new ColumnFilterEvaluatorContext(context, accessorProvider);
            // ignore atomic (or flat) types information
            clipperVisitor.setIgnoreFlatType(true);
            accessorProvider.reset();
            columnFilterEvaluator = columnFilterEvaluatorFactory.create(evaluatorContext);
            filterColumnReaders = accessorProvider.getFilterColumnReaders();
            jobId = context.getJobletContext().getJobId().toString();
        }
        // log normalized filter
        logFilter(jobId, normalizedFilterEvaluator, normalizedEvaluatorFactory.toString());
        // log requested schema
        logSchema(jobId, clippedRoot, SchemaStringBuilderVisitor.RECORD_SCHEMA, fieldNamesDictionary);

        // Primary key readers
        PrimitiveColumnValuesReader[] primaryKeyReaders =
                createPrimaryKeyReaders(input, readerFactory, numberOfPrimaryKeys);

        return new QueryColumnMetadata(datasetType, null, primaryKeyReaders, serializedMetadata, fieldNamesDictionary,
                clippedRoot, readerFactory, valueGetterFactory, normalizedFilterEvaluator, filterValueAccessors,
                columnFilterEvaluator, filterColumnReaders);
    }

    protected static ObjectSchemaNode clip(ARecordType requestedType, ObjectSchemaNode root,
            SchemaClipperVisitor clipperVisitor) {
        ObjectSchemaNode clippedRoot;
        if (ALL_FIELDS_TYPE.getTypeName().equals(requestedType.getTypeName())) {
            clippedRoot = root;
        } else {
            clippedRoot = (ObjectSchemaNode) requestedType.accept(clipperVisitor, root);
        }
        return clippedRoot;
    }

    protected static PrimitiveColumnValuesReader[] createPrimaryKeyReaders(DataInput input,
            IColumnValuesReaderFactory readerFactory, int numberOfPrimaryKeys) throws IOException {
        //skip number of columns
        input.readInt();

        PrimitiveColumnValuesReader[] primaryKeyReaders = new PrimitiveColumnValuesReader[numberOfPrimaryKeys];
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyReaders[i] = (PrimitiveColumnValuesReader) readerFactory.createValueReader(input);
        }
        return primaryKeyReaders;
    }

    protected static void logFilter(String jobId, IColumnFilterEvaluator normalizedFilterEvaluator,
            String filterExpression) {
        if (jobId != null && LOGGER.isDebugEnabled()
                && normalizedFilterEvaluator != TrueColumnFilterEvaluator.INSTANCE) {
            String filterString = normalizedFilterEvaluator == FalseColumnFilterEvaluator.INSTANCE ? "SKIP_ALL"
                    : LogRedactionUtil.userData(filterExpression);
            LOGGER.debug("Filter [{}]: {}", jobId, filterString);
        }
    }

    protected static void logSchema(String jobId, ObjectSchemaNode root, String schemaSource,
            FieldNamesDictionary fieldNamesDictionary) throws HyracksDataException {
        if (jobId != null && LOGGER.isDebugEnabled()) {
            SchemaStringBuilderVisitor schemaBuilder = new SchemaStringBuilderVisitor(fieldNamesDictionary);
            String schema = LogRedactionUtil.userData(schemaBuilder.build(root));
            LOGGER.debug("Queried {} schema [{}]: \n {}", schemaSource, jobId, schema);
        }
    }
}
