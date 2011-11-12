package edu.uci.ics.hyracks.tests.invertedindex;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
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
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class BinaryTokenizerOperatorTest extends AbstractIntegrationTest {

    @Test
    public void tokenizerTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] dblpTitleFileSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/cleanednumbereddblptitles.txt"))) };
        IFileSplitProvider dblpTitleSplitProvider = new ConstantFileSplitProvider(dblpTitleFileSplits);
        RecordDescriptor dblpTitleRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor dblpTitleScanner = new FileScanOperatorDescriptor(spec, dblpTitleSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), dblpTitleRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dblpTitleScanner, NC1_ID);

        RecordDescriptor tokenizerRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                tokenFactory);
        int[] tokenFields = { 1 };
        int[] keyFields = { 0 };
        BinaryTokenizerOperatorDescriptor binaryTokenizer = new BinaryTokenizerOperatorDescriptor(spec,
                tokenizerRecDesc, tokenizerFactory, tokenFields, keyFields);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, binaryTokenizer, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), dblpTitleScanner, 0, binaryTokenizer, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), binaryTokenizer, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}