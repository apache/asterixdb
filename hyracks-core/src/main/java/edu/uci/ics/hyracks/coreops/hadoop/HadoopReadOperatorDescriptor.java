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
package edu.uci.ics.hyracks.coreops.hadoop;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.config.NCConfig;
import edu.uci.ics.hyracks.controller.clustercontroller.IClusterController;
import edu.uci.ics.hyracks.controller.nodecontroller.INodeController;
import edu.uci.ics.hyracks.coreops.file.IRecordReader;
import edu.uci.ics.hyracks.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.hadoop.util.HadoopAdapter;
import edu.uci.ics.hyracks.hadoop.util.HadoopFileSplit;

public class HadoopReadOperatorDescriptor extends AbstractHadoopFileScanOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private String inputFormatClassName;
    private Map<String, String> jobConfMap;

    private static class HDFSCustomReader implements IRecordReader {
        private RecordReader hadoopRecordReader;
        private Class inputKeyClass;
        private Class inputValueClass;
        private Object key;
        private Object value;

        public HDFSCustomReader(Map<String, String> jobConfMap, HadoopFileSplit inputSplit,
            String inputFormatClassName, Reporter reporter) {
            try {
                JobConf conf = DatatypeHelper.hashMap2JobConf((HashMap) jobConfMap);
                FileSystem fileSystem = null;
                try {
                    fileSystem = FileSystem.get(conf);
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

                Class inputFormatClass = Class.forName(inputFormatClassName);
                InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
                hadoopRecordReader = (RecordReader) inputFormat.getRecordReader(getFileSplit(inputSplit), conf,
                    reporter);
                if (hadoopRecordReader instanceof SequenceFileRecordReader) {
                    inputKeyClass = ((SequenceFileRecordReader) hadoopRecordReader).getKeyClass();
                    inputValueClass = ((SequenceFileRecordReader) hadoopRecordReader).getValueClass();
                } else {
                    inputKeyClass = hadoopRecordReader.createKey().getClass();
                    inputValueClass = hadoopRecordReader.createValue().getClass();
                }
                key = inputKeyClass.newInstance();
                value = inputValueClass.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public Class getInputKeyClass() {
            return inputKeyClass;
        }

        public void setInputKeyClass(Class inputKeyClass) {
            this.inputKeyClass = inputKeyClass;
        }

        public Class getInputValueClass() {
            return inputValueClass;
        }

        public void setInputValueClass(Class inputValueClass) {
            this.inputValueClass = inputValueClass;
        }

        @Override
        public void close() {
            try {
                hadoopRecordReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean read(Object[] record) throws Exception {
            if (!hadoopRecordReader.next(key, value)) {
                return false;
            }
            if (record.length == 1) {
                record[0] = value;
            } else {
                record[0] = key;
                record[1] = value;
            }
            return true;
        }

        private FileSplit getFileSplit(HadoopFileSplit hadoopFileSplit) {
            FileSplit fileSplit = new FileSplit(new Path(hadoopFileSplit.getFile()), hadoopFileSplit.getStart(),
                hadoopFileSplit.getLength(), hadoopFileSplit.getHosts());
            return fileSplit;
        }
    }

    public HadoopReadOperatorDescriptor(Map<String, String> jobConfMap, JobSpecification spec,
        HadoopFileSplit[] splits, String inputFormatClassName, RecordDescriptor recordDescriptor) {
        super(spec, splits, recordDescriptor);
        this.inputFormatClassName = inputFormatClassName;
        this.jobConfMap = jobConfMap;
    }

    public HadoopReadOperatorDescriptor(Map<String, String> jobConfMap, InetSocketAddress nameNode,
        JobSpecification spec, String inputFormatClassName, RecordDescriptor recordDescriptor) {
        super(spec, null, recordDescriptor);
        this.inputFormatClassName = inputFormatClassName;
        this.jobConfMap = jobConfMap;
    }

    public HadoopReadOperatorDescriptor(IClusterController clusterController, Map<String, String> jobConfMap,
        JobSpecification spec, String fileSystemURL, String inputFormatClassName, RecordDescriptor recordDescriptor) {
        super(spec, null, recordDescriptor);
        HadoopAdapter hadoopAdapter = HadoopAdapter.getInstance(fileSystemURL);
        String inputPathString = jobConfMap.get("mapred.input.dir");
        String[] inputPaths = inputPathString.split(",");
        Map<String, List<HadoopFileSplit>> blocksToRead = hadoopAdapter.getInputSplits(inputPaths);
        List<HadoopFileSplit> hadoopFileSplits = new ArrayList<HadoopFileSplit>();
        for (String filePath : blocksToRead.keySet()) {
            hadoopFileSplits.addAll(blocksToRead.get(filePath));
        }
        for (HadoopFileSplit hadoopFileSplit : hadoopFileSplits) {
            System.out.println(" Hadoop File Split : " + hadoopFileSplit);
        }
        super.splits = hadoopFileSplits.toArray(new HadoopFileSplit[] {});
        configurePartitionConstraints(clusterController, blocksToRead);
        this.inputFormatClassName = inputFormatClassName;
        this.jobConfMap = jobConfMap;
    }

    private void configurePartitionConstraints(IClusterController clusterController,
        Map<String, List<HadoopFileSplit>> blocksToRead) {
        List<LocationConstraint> locationConstraints = new ArrayList<LocationConstraint>();
        Map<String, INodeController> registry = null;
        try {
            // registry = clusterController.getRegistry();
            // TODO
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<String, String> hostnameToNodeIdMap = new HashMap<String, String>();
        NCConfig ncConfig = null;
        for (String nodeId : registry.keySet()) {
            try {
                ncConfig = ((INodeController) registry.get(nodeId)).getConfiguration();
                String ipAddress = ncConfig.dataIPAddress;
                String hostname = InetAddress.getByName(ipAddress).getHostName();
                System.out.println(" hostname :" + hostname + " nodeid:" + nodeId);
                hostnameToNodeIdMap.put(hostname, nodeId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (String filePath : blocksToRead.keySet()) {
            List<HadoopFileSplit> hadoopFileSplits = blocksToRead.get(filePath);
            for (HadoopFileSplit hadoopFileSplit : hadoopFileSplits) {
                String hostname = hadoopFileSplit.getHosts()[0];
                System.out.println("host name is :" + hostname);
                InetAddress address = null;
                try {
                    address = InetAddress.getByName(hostname);
                    if (address.isLoopbackAddress()) {
                        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
                        while (netInterfaces.hasMoreElements()) {
                            NetworkInterface ni = netInterfaces.nextElement();
                            InetAddress inetAddress = (InetAddress) ni.getInetAddresses().nextElement();
                            if (!inetAddress.isLoopbackAddress()) {
                                address = inetAddress;
                                break;
                            }
                        }
                    }
                    hostname = address.getHostName();
                    System.out.println("cannonical host name hyracks :" + hostname);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String nodeId = hostnameToNodeIdMap.get(hostname);
                System.out.println(" corresponding node id is :" + nodeId);
                LocationConstraint locationConstraint = new AbsoluteLocationConstraint(nodeId);
                locationConstraints.add(locationConstraint);
            }
        }

        PartitionConstraint partitionConstraint = new ExplicitPartitionConstraint(locationConstraints
            .toArray(new LocationConstraint[] {}));
        this.setPartitionConstraint(partitionConstraint);
    }

    @Override
    protected IRecordReader createRecordReader(HadoopFileSplit fileSplit, RecordDescriptor desc) throws Exception {
        Reporter reporter = createReporter();
        IRecordReader recordReader = new HDFSCustomReader(jobConfMap, fileSplit, inputFormatClassName, reporter);
        return recordReader;
    }
}
