///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.asterix.om.query;
//
//import org.apache.asterix.column.assembler.value.IValueGetterFactory;
//import org.apache.asterix.column.filter.FilterAccessorProvider;
//import org.apache.asterix.column.filter.IColumnFilterEvaluator;
//import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
//import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
//import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
//import org.apache.asterix.column.filter.normalized.IColumnFilterNormalizedValueAccessor;
//import org.apache.asterix.column.filter.normalized.IColumnNormalizedFilterEvaluatorFactory;
//import org.apache.asterix.column.metadata.FieldNamesDictionary;
//import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
//import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
//import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
//import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
//import org.apache.asterix.column.values.IColumnValuesReader;
//import org.apache.asterix.column.values.IColumnValuesReaderFactory;
//import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
//import org.apache.asterix.om.types.ARecordType;
//import org.apache.asterix.runtime.projection.FunctionCallInformation;
//import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
//import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
//import org.apache.hyracks.api.context.IHyracksTaskContext;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.api.exceptions.IWarningCollector;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.hyracks.data.std.primitive.IntegerPointable;
//import org.apache.hyracks.dataflow.common.utils.TaskUtil;
//import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
//
//import java.io.ByteArrayInputStream;
//import java.io.DataInput;
//import java.io.DataInputStream;
//import java.io.IOException;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
///**
// * Query column metadata (with metaRecord)
// */
//public final class QueryColumnWithMetaMetadata extends QueryColumnMetadata {
//    private final ColumnAssembler metaAssembler;
//
//    private QueryColumnWithMetaMetadata(ARecordType datasetType, ARecordType metaType,
//            PrimitiveColumnValuesReader[] primaryKeyReaders, IValueReference serializedMetadata,
//            FieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, ObjectSchemaNode metaRoot,
//            IColumnValuesReaderFactory readerFactory, IValueGetterFactory valueGetterFactory,
//            IColumnFilterEvaluator filterEvaluator, List<IColumnFilterNormalizedValueAccessor> filterValueAccessors,
//            IColumnIterableFilterEvaluator columnFilterEvaluator, List<IColumnValuesReader> filterColumnReaders)
//            throws HyracksDataException {
//        super(datasetType, metaType, primaryKeyReaders, serializedMetadata, fieldNamesDictionary, root, readerFactory,
//                valueGetterFactory, filterEvaluator, filterValueAccessors, columnFilterEvaluator, filterColumnReaders);
//        metaAssembler = new ColumnAssembler(metaRoot, metaType, this, readerFactory, valueGetterFactory);
//    }
//
//    @Override
//    public boolean containsMeta() {
//        return true;
//    }
//
//    @Override
//    public int getColumnIndex(int ordinal) {
//        int metaColumnCount = metaAssembler.getNumberOfColumns();
//        if (ordinal >= metaColumnCount) {
//            return assembler.getColumnIndex(ordinal - metaColumnCount);
//        }
//        return metaAssembler.getColumnIndex(ordinal);
//    }
//
//    @Override
//    public int getNumberOfProjectedColumns() {
//        return metaAssembler.getNumberOfColumns() + assembler.getNumberOfColumns();
//    }
//
//    @Override
//    public int getNumberOfColumns() {
//        return getNumberOfProjectedColumns();
//    }
//
//    @Override
//    public AbstractColumnTupleReader createTupleReader() {
//        return new QueryColumnTupleReader(this);
//    }
//
//    public ColumnAssembler getMetaAssembler() {
//        return metaAssembler;
//    }
//
//    /**
//     * Create {@link QueryColumnWithMetaMetadata} that would be used to determine the requested values
//     *
//     * @param datasetType         dataset declared type
//     * @param metaType            meta declared type
//     * @param numberOfPrimaryKeys number of PKs
//     * @param serializedMetadata  inferred metadata (schema)
//     * @param readerFactory       column reader factory
//     * @param valueGetterFactory  value serializer
//     * @param requestedType       the requested schema
//     * @return query metadata
//     */
//    public static QueryColumnWithMetaMetadata create(ARecordType datasetType, ARecordType metaType,
//            int numberOfPrimaryKeys, IValueReference serializedMetadata, IColumnValuesReaderFactory readerFactory,
//            IValueGetterFactory valueGetterFactory, ARecordType requestedType,
//            Map<String, FunctionCallInformation> functionCallInfo, ARecordType metaRequestedType,
//            Map<String, FunctionCallInformation> metaFunctionCallInfo,
//            IColumnNormalizedFilterEvaluatorFactory normalizedEvaluatorFactory,
//            IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory, IWarningCollector warningCollector,
//            IHyracksTaskContext context) throws IOException {
//        byte[] bytes = serializedMetadata.getByteArray();
//        int offset = serializedMetadata.getStartOffset();
//        int length = serializedMetadata.getLength();
//
//        int fieldNamesStart = offset + IntegerPointable.getInteger(bytes, offset + FIELD_NAMES_POINTER);
//        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, length));
//
//        //FieldNames
//        FieldNamesDictionary fieldNamesDictionary = FieldNamesDictionary.deserialize(input);
//
//        //Schema
//        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);
//        ObjectSchemaNode metaRoot = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);
//
//        //Clip dataset schema
//        SchemaClipperVisitor clipperVisitor =
//                new SchemaClipperVisitor(fieldNamesDictionary, functionCallInfo, warningCollector);
//        ObjectSchemaNode clippedRoot = clip(requestedType, root, clipperVisitor);
//
//        //Clip meta schema
//        SchemaClipperVisitor metaClipperVisitor =
//                new SchemaClipperVisitor(fieldNamesDictionary, metaFunctionCallInfo, warningCollector);
//        ObjectSchemaNode metaClippedRoot = clip(metaRequestedType, metaRoot, metaClipperVisitor);
//
//        IColumnFilterEvaluator normalizedFilterEvaluator = TrueColumnFilterEvaluator.INSTANCE;
//        IColumnIterableFilterEvaluator columnFilterEvaluator = TrueColumnFilterEvaluator.INSTANCE;
//        List<IColumnValuesReader> filterColumnReaders = Collections.emptyList();
//        List<IColumnFilterNormalizedValueAccessor> filterValueAccessors = Collections.emptyList();
//        if (context != null) {
//            FilterAccessorProvider filterAccessorProvider =
//                    new FilterAccessorProvider(root, clipperVisitor, readerFactory, valueGetterFactory);
//            TaskUtil.put(FilterAccessorProvider.FILTER_ACCESSOR_PROVIDER_KEY, filterAccessorProvider, context);
//            // Min/Max filters in page0
//            normalizedFilterEvaluator = normalizedEvaluatorFactory.create(filterAccessorProvider);
//            filterValueAccessors = filterAccessorProvider.getFilterAccessors();
//
//            // Filter columns (columns appeared in WHERE clause)
//            IEvaluatorContext evaluatorContext = new EvaluatorContext(context);
//            // ignore atomic (or flat) types information
//            clipperVisitor.setIgnoreFlatType(true);
//            filterAccessorProvider.reset();
//            columnFilterEvaluator = columnFilterEvaluatorFactory.create(filterAccessorProvider, evaluatorContext);
//            filterColumnReaders = filterAccessorProvider.getFilterColumnReaders();
//        }
//
//        // Primary key readers
//        PrimitiveColumnValuesReader[] primaryKeyReaders =
//                createPrimaryKeyReaders(input, readerFactory, numberOfPrimaryKeys);
//
//        // log requested schema for record
//        logSchema(clippedRoot, SchemaStringBuilderVisitor.RECORD_SCHEMA, fieldNamesDictionary);
//        // log requested schema for meta-record
//        logSchema(metaClippedRoot, SchemaStringBuilderVisitor.META_RECORD_SCHEMA, fieldNamesDictionary);
//
//        return new QueryColumnWithMetaMetadata(datasetType, metaType, primaryKeyReaders, serializedMetadata,
//                fieldNamesDictionary, clippedRoot, metaClippedRoot, readerFactory, valueGetterFactory,
//                normalizedFilterEvaluator, filterValueAccessors, columnFilterEvaluator, filterColumnReaders);
//    }
//}
