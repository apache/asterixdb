/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.tests.invertedindex;

import java.io.DataOutput;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestIndexRegistryProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class WordInvertedIndexTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<IIndex> indexRegistryProvider = new TestIndexRegistryProvider();
    private IIndexDataflowHelperFactory btreeDataflowHelperFactory = new BTreeDataflowHelperFactory();

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");
    private final String dateString = simpleDateFormat.format(new Date());
    private final String primaryFileName = System.getProperty("java.io.tmpdir") + sep + "primaryBtree" + dateString;
    private final String btreeFileName = System.getProperty("java.io.tmpdir") + sep + "invIndexBtree" + dateString;
    private final String invListsFileName = System.getProperty("java.io.tmpdir") + sep + "invIndexLists" + dateString;

    private IFileSplitProvider primaryFileSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });
    private IFileSplitProvider btreeFileSplitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(
            NC1_ID, new FileReference(new File(btreeFileName))) });
    private IFileSplitProvider invListsFileSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(invListsFileName))) });

    // Primary BTree index.
    private int primaryFieldCount = 2;
    private ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
    private int primaryKeyFieldCount = 1;
    private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
    private TypeAwareTupleWriterFactory primaryTupleWriterFactory = new TypeAwareTupleWriterFactory(primaryTypeTraits);
    private ITreeIndexFrameFactory primaryInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(
            primaryTupleWriterFactory);
    private ITreeIndexFrameFactory primaryLeafFrameFactory = new BTreeNSMLeafFrameFactory(primaryTupleWriterFactory);
    private RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    // Inverted index BTree dictionary.
    private ITypeTrait[] tokenTypeTraits = new ITypeTrait[1];
    private IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[1];

    // Inverted index stuff.
    private int invListElementFieldCount = 1;
    private ITypeTrait[] invListsTypeTraits = new ITypeTrait[invListElementFieldCount];
    private IBinaryComparatorFactory[] invListsComparatorFactories = new IBinaryComparatorFactory[invListElementFieldCount];
    private RecordDescriptor tokenizerRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
    private RecordDescriptor invListsRecDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

    // Tokenizer stuff.
    private ITokenFactory tokenFactory = new UTF8WordTokenFactory();
    private IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
            tokenFactory);

    @Before
    public void setup() throws Exception {
        // Field declarations and comparators for primary BTree index.
        primaryTypeTraits[0] = ITypeTrait.INTEGER_TYPE_TRAIT;
        primaryTypeTraits[1] = ITypeTrait.VARLEN_TYPE_TRAIT;
        primaryComparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        // Field declarations and comparators for tokens.
        tokenTypeTraits[0] = ITypeTrait.VARLEN_TYPE_TRAIT;
        tokenComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // Field declarations and comparators for inverted lists.
        invListsTypeTraits[0] = ITypeTrait.INTEGER_TYPE_TRAIT;
        invListsComparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        loadPrimaryIndex();
        printPrimaryIndex();
        loadInvertedIndex();
    }

    @Test
    public void testConjunctiveSearcher() throws Exception {
        IInvertedIndexSearchModifierFactory conjunctiveSearchModifierFactory = new ConjunctiveSearchModifierFactory();
        searchInvertedIndex("of", conjunctiveSearchModifierFactory);
        searchInvertedIndex("3d", conjunctiveSearchModifierFactory);
        searchInvertedIndex("of the human", conjunctiveSearchModifierFactory);
    }

    private IOperatorDescriptor createFileScanOp(JobSpecification spec) {
        FileSplit[] dblpTitleFileSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/cleanednumbereddblptitles.txt"))) };
        IFileSplitProvider dblpTitleSplitProvider = new ConstantFileSplitProvider(dblpTitleFileSplits);
        RecordDescriptor dblpTitleRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor dblpTitleScanner = new FileScanOperatorDescriptor(spec, dblpTitleSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), dblpTitleRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dblpTitleScanner, NC1_ID);
        return dblpTitleScanner;
    }

    private IOperatorDescriptor createPrimaryBulkLoadOp(JobSpecification spec) {
        int[] fieldPermutation = { 0, 1 };
        TreeIndexBulkLoadOperatorDescriptor primaryBtreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, indexRegistryProvider, primaryFileSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, fieldPermutation, 0.7f,
                btreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeBulkLoad, NC1_ID);
        return primaryBtreeBulkLoad;
    }

    private IOperatorDescriptor createScanKeyProviderOp(JobSpecification spec) throws HyracksDataException {
        // build dummy tuple containing nothing
        ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryKeyFieldCount * 2);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos);
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);
        return keyProviderOp;
    }

    private IOperatorDescriptor createPrimaryScanOp(JobSpecification spec) throws HyracksDataException {
        int[] lowKeyFields = null; // - infinity
        int[] highKeyFields = null; // + infinity
        BTreeSearchOperatorDescriptor primaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, indexRegistryProvider, primaryFileSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, true, lowKeyFields,
                highKeyFields, true, true, btreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeSearchOp, NC1_ID);
        return primaryBtreeSearchOp;
    }

    private void loadPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        // Assuming that the data is pre-sorted on the key. No need to sort
        // before bulk load.
        IOperatorDescriptor fileScanOp = createFileScanOp(spec);
        IOperatorDescriptor primaryBulkLoad = createPrimaryBulkLoadOp(spec);
        spec.connect(new OneToOneConnectorDescriptor(spec), fileScanOp, 0, primaryBulkLoad, 0);
        spec.addRoot(primaryBulkLoad);
        runTest(spec);
    }

    private void printPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor keyProviderOp = createScanKeyProviderOp(spec);
        IOperatorDescriptor primaryScanOp = createPrimaryScanOp(spec);
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, printer, 0);
        spec.addRoot(printer);
        runTest(spec);
    }

    private IOperatorDescriptor createExternalSortOp(JobSpecification spec, int[] sortFields,
            RecordDescriptor outputRecDesc) {
        ExternalSortOperatorDescriptor externalSortOp = new ExternalSortOperatorDescriptor(spec, 1000, sortFields,
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE }, outputRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, externalSortOp, NC1_ID);
        return externalSortOp;
    }

    private IOperatorDescriptor createBinaryTokenizerOp(JobSpecification spec, int[] tokenFields, int[] keyFields) {
        BinaryTokenizerOperatorDescriptor binaryTokenizer = new BinaryTokenizerOperatorDescriptor(spec,
                tokenizerRecDesc, tokenizerFactory, tokenFields, keyFields);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, binaryTokenizer, NC1_ID);
        return binaryTokenizer;
    }

    private IOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec, int[] fieldPermutation) {
        InvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new InvertedIndexBulkLoadOperatorDescriptor(spec,
                fieldPermutation, storageManager, btreeFileSplitProvider, invListsFileSplitProvider,
                indexRegistryProvider, tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits,
                invListsComparatorFactories, btreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, invIndexBulkLoadOp, NC1_ID);
        return invIndexBulkLoadOp;
    }

    public void loadInvertedIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor keyProviderOp = createScanKeyProviderOp(spec);
        IOperatorDescriptor primaryScanOp = createPrimaryScanOp(spec);
        int[] tokenFields = { 1 };
        int[] keyFields = { 0 };
        IOperatorDescriptor binaryTokenizerOp = createBinaryTokenizerOp(spec, tokenFields, keyFields);
        int[] sortFields = { 0, 1 };
        IOperatorDescriptor externalSortOp = createExternalSortOp(spec, sortFields, tokenizerRecDesc);
        int[] fieldPermutation = { 0, 1 };
        IOperatorDescriptor invIndexBulkLoadOp = createInvertedIndexBulkLoadOp(spec, fieldPermutation);
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, binaryTokenizerOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), binaryTokenizerOp, 0, externalSortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), externalSortOp, 0, invIndexBulkLoadOp, 0);
        spec.addRoot(invIndexBulkLoadOp);
        runTest(spec);
    }

    private IOperatorDescriptor createQueryProviderOp(JobSpecification spec, String queryString)
            throws HyracksDataException {
        // Build tuple with exactly one field, which is the query,
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize(queryString, dos);
        tb.addFieldEndOffset();
        ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);
        ConstantTupleSourceOperatorDescriptor queryProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                queryRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, queryProviderOp, NC1_ID);
        return queryProviderOp;
    }

    private IOperatorDescriptor createInvertedIndexSearchOp(JobSpecification spec,
            IInvertedIndexSearchModifierFactory searchModifierFactory) {
        InvertedIndexSearchOperatorDescriptor invIndexSearchOp = new InvertedIndexSearchOperatorDescriptor(spec, 0,
                storageManager, btreeFileSplitProvider, invListsFileSplitProvider, indexRegistryProvider,
                tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits, invListsComparatorFactories,
                btreeDataflowHelperFactory, tokenizerFactory, searchModifierFactory, invListsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, invIndexSearchOp, NC1_ID);
        return invIndexSearchOp;
    }

    public void searchInvertedIndex(String queryString, IInvertedIndexSearchModifierFactory searchModifierFactory)
            throws Exception {
        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor queryProviderOp = createQueryProviderOp(spec, queryString);
        IOperatorDescriptor invIndexSearchOp = createInvertedIndexSearchOp(spec, searchModifierFactory);
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        spec.connect(new OneToOneConnectorDescriptor(spec), queryProviderOp, 0, invIndexSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), invIndexSearchOp, 0, printer, 0);
        spec.addRoot(printer);
        runTest(spec);
    }
}
