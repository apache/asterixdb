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
package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hivesterix.runtime.operator.filescan.HiveKeyValueParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

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
        try {
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

            // setup the run time operator and constraints
            JobConf conf = ConfUtil.getJobConf(fileDesc.getInputFileFormatClass(), filePath);
            String[] locConstraints = ConfUtil.getNCs();
            Map<String, NodeControllerInfo> ncNameToNcInfos = ConfUtil.getNodeControllerInfo();
            ClusterTopology topology = ConfUtil.getClusterTopology();
            Scheduler scheduler = new Scheduler(ncNameToNcInfos, topology);
            InputSplit[] splits = conf.getInputFormat().getSplits(conf, locConstraints.length);
            String[] schedule = scheduler.getLocationConstraints(splits);
            IOperatorDescriptor scanner = new HDFSReadOperatorDescriptor(jobSpec, recDescriptor, conf, splits,
                    schedule, new HiveKeyValueParserFactory(fileDesc, conf, outputColumnsOffset));

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner,
                    new AlgebricksAbsolutePartitionConstraint(locConstraints));
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
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
