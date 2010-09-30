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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;

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
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = createOrdersFileSplits();
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        ordScanner.setPartitionConstraint(createRRPartitionConstraint(2));

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        custScanner.setPartitionConstraint(createRRPartitionConstraint(2));

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 0 },
                new int[] { 1 }, new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, custOrderJoinDesc,
                6000000);
        join.setPartitionConstraint(new PartitionCountConstraint(4));

        RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        HashGroupOperatorDescriptor gby = new HashGroupOperatorDescriptor(
                spec,
                new int[] { 6 },
                new FieldHashPartitionComputerFactory(new int[] { 6 },
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }),
                groupResultDesc, 16);
        gby.setPartitionConstraint(new PartitionCountConstraint(4));

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        printer.setPartitionConstraint(new PartitionCountConstraint(4));

        IConnectorDescriptor ordJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 6 },
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(joinGroupConn, join, 0, gby, 0);

        IConnectorDescriptor gbyPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(gbyPrinterConn, gby, 0, printer, 0);

        spec.addRoot(printer);
        return spec;
    }

    private static FileSplit[] createOrdersFileSplits() {
        FileSplit fss[] = new FileSplit[2];
        for (int i = 0; i < fss.length; ++i) {
            fss[i] = new FileSplit("foo", new File("data/tpch0.001/orders-part" + (i + 1) + ".tbl"));
        }
        return fss;
    }

    private static FileSplit[] createCustomerFileSplits() {
        FileSplit fss[] = new FileSplit[2];
        for (int i = 0; i < fss.length; ++i) {
            fss[i] = new FileSplit("foo", new File("data/tpch0.001/customer-part" + (i + 1) + ".tbl"));
        }
        return fss;
    }

    private static final LocationConstraint[] LCS = { new AbsoluteLocationConstraint("NC1"),
            new AbsoluteLocationConstraint("NC2") };

    private static PartitionConstraint createRRPartitionConstraint(int k) {
        LocationConstraint[] lcs = new LocationConstraint[2];
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