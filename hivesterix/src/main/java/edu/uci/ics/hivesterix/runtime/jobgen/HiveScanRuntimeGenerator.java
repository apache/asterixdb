package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.runtime.config.ConfUtil;
import edu.uci.ics.hivesterix.runtime.operator.filescan.HiveFileScanOperatorDescriptor;
import edu.uci.ics.hivesterix.runtime.operator.filescan.HiveFileSplitProvider;
import edu.uci.ics.hivesterix.runtime.operator.filescan.HiveTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class HiveScanRuntimeGenerator {

    private PartitionDesc fileDesc;

    private transient Path filePath;

    private String filePathName;

    private Properties properties;

    public HiveScanRuntimeGenerator(PartitionDesc path) {
        fileDesc = path;
        properties = fileDesc.getProperties();

        String inputPath = (String) properties.getProperty("location");

        if (inputPath.startsWith("file:")) {
            // Windows
            String[] strs = inputPath.split(":");
            filePathName = strs[strs.length - 1];
        } else {
            // Linux
            filePathName = inputPath;
        }

        filePath = new Path(filePathName);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRuntimeOperatorAndConstraint(
            IDataSource dataSource, List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables,
            boolean projectPushed, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        // get the correct delimiter from Hive metastore or other data
        // structures
        IOperatorSchema propagatedSchema = new HiveOperatorSchema();

        List<LogicalVariable> outputVariables = projectPushed ? projectVariables : scanVariables;
        for (LogicalVariable var : outputVariables)
            propagatedSchema.addVariable(var);

        int[] outputColumnsOffset = new int[scanVariables.size()];
        int i = 0;
        for (LogicalVariable var : scanVariables)
            if (outputVariables.contains(var)) {
                int offset = outputVariables.indexOf(var);
                outputColumnsOffset[i++] = offset;
            } else
                outputColumnsOffset[i++] = -1;

        Object[] schemaTypes = dataSource.getSchemaTypes();
        // get record descriptor
        RecordDescriptor recDescriptor = mkRecordDescriptor(propagatedSchema, schemaTypes, context);

        // setup the run time operator
        JobConf conf = ConfUtil.getJobConf(fileDesc.getInputFileFormatClass(), filePath);
        int clusterSize = ConfUtil.getNCs().length;
        IFileSplitProvider fsprovider = new HiveFileSplitProvider(conf, filePathName, clusterSize);
        ITupleParserFactory tupleParserFactory = new HiveTupleParserFactory(fileDesc, conf, outputColumnsOffset);
        HiveFileScanOperatorDescriptor opDesc = new HiveFileScanOperatorDescriptor(jobSpec, fsprovider,
                tupleParserFactory, recDescriptor);

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(opDesc, opDesc.getPartitionConstraint());
    }

    private static RecordDescriptor mkRecordDescriptor(IOperatorSchema opSchema, Object[] types, JobGenContext context)
            throws AlgebricksException {
        ISerializerDeserializer[] fields = new ISerializerDeserializer[opSchema.getSize()];
        ISerializerDeserializerProvider sdp = context.getSerializerDeserializerProvider();
        int size = opSchema.getSize();
        for (int i = 0; i < size; i++) {
            Object t = types[i];
            fields[i] = sdp.getSerializerDeserializer(t);
            i++;
        }
        return new RecordDescriptor(fields);
    }

}
