package edu.uci.ics.asterix.file;

import java.io.DataOutput;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.context.AsterixTreeRegistryProvider;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TestSecondaryIndexJob {

    private static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();
    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
    }

    public static int DEFAULT_INPUT_DATA_COLUMN = 0;
    public static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    @SuppressWarnings("unchecked")
    public JobSpecification createJobSpec() throws AsterixException, HyracksDataException {

        JobSpecification spec = new JobSpecification();

        // ---------- START GENERAL BTREE STUFF

        IIndexRegistryProvider<IIndex> btreeRegistryProvider = AsterixTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;

        // ---------- END GENERAL BTREE STUFF

        List<String> nodeGroup = new ArrayList<String>();
        nodeGroup.add("nc1");
        nodeGroup.add("nc2");

        // ---------- START KEY PROVIDER OP

        // TODO: should actually be empty tuple source
        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1); // just one dummy field
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        AObjectSerializerDeserializer.INSTANCE.serialize(new AString("Jodi Rotruck"), dos); // dummy
        // field
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { AObjectSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        String[] keyProviderOpLocationConstraint = new String[nodeGroup.size()];
        for (int p = 0; p < nodeGroup.size(); p++) {
            keyProviderOpLocationConstraint[p] = nodeGroup.get(p);
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, keyProviderOpLocationConstraint);

        // ---------- END KEY PROVIDER OP

        // ---------- START SECONRARY INDEX SCAN

        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[2];
        secondaryTypeTraits[0] = new ITypeTraits() {

            @Override
            public boolean isFixedLength() {
                return false;
            }

            @Override
            public int getFixedLength() {
                return -1;
            }
        };

        secondaryTypeTraits[1] = new ITypeTraits() {

            @Override
            public boolean isFixedLength() {
                return true;
            }

            @Override
            public int getFixedLength() {
                return 5;
            }
        };

        ITreeIndexFrameFactory interiorFrameFactory = AqlMetadataProvider
                .createBTreeNSMInteriorFrameFactory(secondaryTypeTraits);
        ITreeIndexFrameFactory leafFrameFactory = AqlMetadataProvider.createBTreeNSMLeafFrameFactory(secondaryTypeTraits);

        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[2];
        secondaryRecFields[0] = AObjectSerializerDeserializer.INSTANCE;
        secondaryRecFields[1] = AObjectSerializerDeserializer.INSTANCE;
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[2];
        secondaryComparatorFactories[0] = AObjectAscBinaryComparatorFactory.INSTANCE;
        secondaryComparatorFactories[1] = AObjectAscBinaryComparatorFactory.INSTANCE;

        int[] lowKeyFields = null; // -infinity
        int[] highKeyFields = null; // +infinity
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(secondaryRecFields);
        // TODO: change file splits according to mount points in cluster config
        IFileSplitProvider secondarySplitProvider = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit("nc1", new FileReference(new File("/tmp/nc1/demo1112/Customers_idx_NameBtreeIndex"))),
                new FileSplit("nc2", new FileReference(new File("/tmp/nc2/demo1112/Customers_idx_NameBtreeIndex"))) });
        BTreeSearchOperatorDescriptor secondarySearchOp = new BTreeSearchOperatorDescriptor(spec, secondaryRecDesc,
                storageManager, btreeRegistryProvider, secondarySplitProvider, interiorFrameFactory, leafFrameFactory,
                secondaryTypeTraits, secondaryComparatorFactories, true, lowKeyFields, highKeyFields, true, true,
                new BTreeDataflowHelperFactory());
        String[] secondarySearchOpLocationConstraint = new String[nodeGroup.size()];
        for (int p = 0; p < nodeGroup.size(); p++) {
            secondarySearchOpLocationConstraint[p] = nodeGroup.get(p);
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondarySearchOp,
                secondarySearchOpLocationConstraint);

        // ---------- END SECONDARY INDEX SCAN

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        String[] printerLocationConstraint = new String[nodeGroup.size()];
        for (int p = 0; p < nodeGroup.size(); p++) {
            printerLocationConstraint[p] = nodeGroup.get(p);
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, printerLocationConstraint);

        // ---------- START CONNECT THE OPERATORS

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondarySearchOp, 0, printer, 0);

        // ---------- END CONNECT THE OPERATORS

        spec.addRoot(printer);

        return spec;
    }

    public static void main(String[] args) throws Exception {
        String host;
        String appName;
        String ddlFile;

        switch (args.length) {
            case 0: {
                host = "127.0.0.1";
                appName = "asterix";
                ddlFile = "/home/nicnic/workspace/asterix/trunk/asterix/asterix-app/src/test/resources/demo0927/local/create-index.aql";
                System.out.println("No arguments specified, using defauls:");
                System.out.println("HYRACKS HOST: " + host);
                System.out.println("APPNAME:      " + appName);
                System.out.println("DDLFILE:      " + ddlFile);
            }
                break;

            case 3: {
                host = args[0];
                appName = args[1];
                ddlFile = args[2];
            }
                break;

            default: {
                System.out.println("USAGE:");
                System.out.println("ARG 1: Hyracks Host (IP or Hostname)");
                System.out.println("ARG 2: Application Name (e.g., asterix)");
                System.out.println("ARG 3: DDL File");
                host = null;
                appName = null;
                ddlFile = null;
                System.exit(0);
            }
                break;
        }

        int port = 1098;
        IHyracksClientConnection hcc = new HyracksConnection(host, port);

        TestSecondaryIndexJob tij = new TestSecondaryIndexJob();
        JobSpecification jobSpec = tij.createJobSpec();
        JobId jobId = hcc.createJob("asterix", jobSpec);

        long start = System.currentTimeMillis();
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }
}
