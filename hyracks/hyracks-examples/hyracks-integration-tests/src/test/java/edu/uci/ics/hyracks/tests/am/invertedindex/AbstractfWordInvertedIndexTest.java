/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.tests.am.invertedindex;

import java.io.DataOutput;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;
import edu.uci.ics.hyracks.test.support.TestIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

@SuppressWarnings("rawtypes")
public abstract class AbstractfWordInvertedIndexTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    protected static final Map<String, String> MERGE_POLICY_PROPERTIES;
    static {
        MERGE_POLICY_PROPERTIES = new HashMap<String, String>();
        MERGE_POLICY_PROPERTIES.put("num-components", "3");
    }

    protected IVirtualBufferCacheProvider virtualBufferCacheProvider = new TestVirtualBufferCacheProvider(
            DEFAULT_MEM_PAGE_SIZE, DEFAULT_MEM_NUM_PAGES);
    protected IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    protected IIndexLifecycleManagerProvider lcManagerProvider = new TestIndexLifecycleManagerProvider();
    protected IIndexDataflowHelperFactory btreeDataflowHelperFactory = new BTreeDataflowHelperFactory();
    protected IIndexDataflowHelperFactory invertedIndexDataflowHelperFactory;

    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String sep = System.getProperty("file.separator");
    protected final String dateString = simpleDateFormat.format(new Date());
    protected final String primaryFileName = System.getProperty("java.io.tmpdir") + sep + "primaryBtree" + dateString;
    protected final String btreeFileName = System.getProperty("java.io.tmpdir") + sep + "invIndexBtree" + dateString;

    protected IFileSplitProvider primaryFileSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });
    protected IFileSplitProvider btreeFileSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(btreeFileName))) });

    // Primary BTree index.
    protected int primaryFieldCount = 2;
    protected ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
    protected int primaryKeyFieldCount = 1;
    protected IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
    protected RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    // Inverted index BTree dictionary.
    protected ITypeTraits[] tokenTypeTraits;
    protected IBinaryComparatorFactory[] tokenComparatorFactories;

    // Inverted index stuff.
    protected int invListElementFieldCount = 1;
    protected ITypeTraits[] invListsTypeTraits = new ITypeTraits[invListElementFieldCount];
    protected IBinaryComparatorFactory[] invListsComparatorFactories = new IBinaryComparatorFactory[invListElementFieldCount];
    protected RecordDescriptor invListsRecDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
    protected RecordDescriptor tokenizerRecDesc;

    // Tokenizer stuff.
    protected ITokenFactory tokenFactory = new UTF8WordTokenFactory();
    protected IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
            tokenFactory);

    // Sorting stuff.
    IBinaryComparatorFactory[] sortComparatorFactories;

    @Before
    public void setup() throws Exception {
        prepare();

        // Field declarations and comparators for primary BTree index.
        primaryTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Field declarations and comparators for inverted lists.
        invListsTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        invListsComparatorFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        createPrimaryIndex();
        loadPrimaryIndex();
        printPrimaryIndex();
        createInvertedIndex();
        loadInvertedIndex();
    }

    protected abstract void prepare();

    protected abstract boolean addNumTokensKey();

    public void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TransientLocalResourceFactoryProvider localResourceFactoryProvider = new TransientLocalResourceFactoryProvider();
        TreeIndexCreateOperatorDescriptor primaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec, storageManager,
                lcManagerProvider, primaryFileSplitProvider, primaryTypeTraits, primaryComparatorFactories, null,
                btreeDataflowHelperFactory, localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }

    public void createInvertedIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        ILocalResourceFactoryProvider localResourceFactoryProvider = new TransientLocalResourceFactoryProvider();
        LSMInvertedIndexCreateOperatorDescriptor invIndexCreateOp = new LSMInvertedIndexCreateOperatorDescriptor(spec,
                storageManager, btreeFileSplitProvider, lcManagerProvider, tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, invListsComparatorFactories, tokenizerFactory, invertedIndexDataflowHelperFactory,
                localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, invIndexCreateOp, NC1_ID);
        spec.addRoot(invIndexCreateOp);
        runTest(spec);
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
                storageManager, lcManagerProvider, primaryFileSplitProvider, primaryTypeTraits,
                primaryComparatorFactories, null, fieldPermutation, 0.7f, true, 1000L, true,
                btreeDataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
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
                storageManager, lcManagerProvider, primaryFileSplitProvider, primaryTypeTraits,
                primaryComparatorFactories, null, lowKeyFields, highKeyFields, true, true, btreeDataflowHelperFactory,
                false, NoOpOperationCallbackFactory.INSTANCE);
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
                sortComparatorFactories, outputRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, externalSortOp, NC1_ID);
        return externalSortOp;
    }

    private IOperatorDescriptor createBinaryTokenizerOp(JobSpecification spec, int docField, int[] keyFields) {
        BinaryTokenizerOperatorDescriptor binaryTokenizer = new BinaryTokenizerOperatorDescriptor(spec,
                tokenizerRecDesc, tokenizerFactory, docField, keyFields, addNumTokensKey());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, binaryTokenizer, NC1_ID);
        return binaryTokenizer;
    }

    private IOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec, int[] fieldPermutation) {
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new LSMInvertedIndexBulkLoadOperatorDescriptor(
                spec, fieldPermutation, true, 1000L, true, storageManager, btreeFileSplitProvider, lcManagerProvider,
                tokenTypeTraits, tokenComparatorFactories, invListsTypeTraits, invListsComparatorFactories,
                tokenizerFactory, invertedIndexDataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, invIndexBulkLoadOp, NC1_ID);
        return invIndexBulkLoadOp;
    }

    public void loadInvertedIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor keyProviderOp = createScanKeyProviderOp(spec);
        IOperatorDescriptor primaryScanOp = createPrimaryScanOp(spec);
        int docField = 1;
        int[] keyFields = { 0 };
        IOperatorDescriptor binaryTokenizerOp = createBinaryTokenizerOp(spec, docField, keyFields);
        int[] sortFields = new int[sortComparatorFactories.length];
        int[] fieldPermutation = new int[sortComparatorFactories.length];
        for (int i = 0; i < sortFields.length; i++) {
            sortFields[i] = i;
            fieldPermutation[i] = i;
        }
        IOperatorDescriptor externalSortOp = createExternalSortOp(spec, sortFields, tokenizerRecDesc);
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
        LSMInvertedIndexSearchOperatorDescriptor invIndexSearchOp = new LSMInvertedIndexSearchOperatorDescriptor(spec,
                0, storageManager, btreeFileSplitProvider, lcManagerProvider, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListsComparatorFactories,
                invertedIndexDataflowHelperFactory, tokenizerFactory, searchModifierFactory, invListsRecDesc, false,
                NoOpOperationCallbackFactory.INSTANCE);
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
