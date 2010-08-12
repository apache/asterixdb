package edu.uci.ics.hyracks.examples.tpch.client;

import java.io.File;
import java.util.UUID;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ChoiceLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.CSVFileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.examples.tpch.helper.CountAccumulatingAggregatorFactory;

public class Main {
    public static void main(String[] args) throws Exception {
        String appName = args[0];
        String host;
        int port = 1099;
        switch (args.length) {
            case 3:
                port = Integer.parseInt(args[2]);
            case 2:
                host = args[1];
                break;
            default:
                System.err.println("One or Two arguments expected: <cchost> [<ccport>]");
                return;
        }
        IHyracksClientConnection hcc = new HyracksRMIConnection(host, port);

        JobSpecification job = createJob();

        long start = System.currentTimeMillis();
        UUID jobId = hcc.createJob(appName, job);
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob() {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = createCustomerFileSplits();
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = createOrdersFileSplits();
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        CSVFileScanOperatorDescriptor ordScanner = new CSVFileScanOperatorDescriptor(spec, ordersSplits, ordersDesc,
                '|', "'\"");
        ordScanner.setPartitionConstraint(createRRPartitionConstraint(10));

        CSVFileScanOperatorDescriptor custScanner = new CSVFileScanOperatorDescriptor(spec, custSplits, custDesc, '|',
                "'\"");
        custScanner.setPartitionConstraint(createRRPartitionConstraint(10));

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 0 },
                new int[] { 1 }, new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE },
                new IBinaryComparatorFactory[] { StringBinaryComparatorFactory.INSTANCE }, custOrderJoinDesc, 6000000);
        join.setPartitionConstraint(new PartitionCountConstraint(40));

        RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        HashGroupOperatorDescriptor gby = new HashGroupOperatorDescriptor(spec, new int[] { 6 },
                new FieldHashPartitionComputerFactory(new int[] { 6 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { StringBinaryComparatorFactory.INSTANCE },
                new CountAccumulatingAggregatorFactory(), groupResultDesc, 16);
        gby.setPartitionConstraint(new PartitionCountConstraint(40));

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        printer.setPartitionConstraint(new PartitionCountConstraint(40));

        IConnectorDescriptor ordJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 6 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(joinGroupConn, join, 0, gby, 0);

        IConnectorDescriptor gbyPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(gbyPrinterConn, gby, 0, printer, 0);

        spec.addRoot(printer);
        return spec;
    }

    private static FileSplit[] createOrdersFileSplits() {
        FileSplit fss[] = new FileSplit[40];
        for (int i = 0; i < fss.length; ++i) {
            fss[i] = new FileSplit("foo", new File("/home/ubuntu/vinayakb/data/tpch.40.splits/disk" + (i % 4)
                    + "/orders" + i + ".dat"));
        }
        return fss;
    }

    private static FileSplit[] createCustomerFileSplits() {
        FileSplit fss[] = new FileSplit[40];
        for (int i = 0; i < fss.length; ++i) {
            fss[i] = new FileSplit("foo", new File("/home/ubuntu/vinayakb/data/tpch.40.splits/disk" + (i % 4)
                    + "/customer" + i + ".dat"));
        }
        return fss;
    }

    private static final LocationConstraint[] LCS = { new AbsoluteLocationConstraint("asterix-001"),
            new AbsoluteLocationConstraint("asterix-002"), new AbsoluteLocationConstraint("asterix-003"),
            new AbsoluteLocationConstraint("asterix-004"), new AbsoluteLocationConstraint("asterix-005"),
            new AbsoluteLocationConstraint("asterix-006"), new AbsoluteLocationConstraint("asterix-007"),
            new AbsoluteLocationConstraint("asterix-008"), new AbsoluteLocationConstraint("asterix-009"),
            new AbsoluteLocationConstraint("asterix-010"), };

    private static PartitionConstraint createRRPartitionConstraint(int k) {
        LocationConstraint[] lcs = new LocationConstraint[40];
        for (int i = 0; i < lcs.length; ++i) {
            lcs[i] = createRRSteppedChoiceConstraint(i, k);
        }
        return new ExplicitPartitionConstraint(lcs);
    }

    private static LocationConstraint createRRSteppedChoiceConstraint(int index, int choices) {
        LocationConstraint[] lcs = new LocationConstraint[choices];
        for (int i = 0; i < choices; ++i) {
            lcs[i] = LCS[(index + i) % LCS.length];
        }
        return new ChoiceLocationConstraint(lcs);
    }
}