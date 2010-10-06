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
package edu.uci.ics.hyracks.examples.btree.client;

import java.util.UUID;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.BTreeRegistryProvider;
import edu.uci.ics.hyracks.examples.btree.helper.BufferCacheProvider;
import edu.uci.ics.hyracks.examples.btree.helper.FileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDiskOrderScanOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IFileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;

// This example will load a secondary index with <key, primary-index key> pairs
// We require an existing primary index built with PrimaryIndexBulkLoadExample

public class SecondaryIndexBulkLoadExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;
        
        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;
                
        @Option(name = "-primary-btreename", usage = "Name of primary-index B-Tree to load from", required = true)
        public String primaryBtreeName;
        
        @Option(name = "-btreename", usage = "B-Tree file name for secondary index to be built", required = true)
        public String btreeName;
        
        @Option(name = "-sortbuffer-size", usage = "Sort buffer size in frames (default: 32768)", required = false)
        public int sbSize = 32768;
    }
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job = createJob(options);

        long start = System.currentTimeMillis();
        UUID jobId = hcc.createJob(options.app, job);
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }
    
    private static JobSpecification createJob(Options options) {
    	
    	JobSpecification spec = new JobSpecification();

    	String[] splitNCs = options.ncs.split(",");
    	
    	// schema of tuples that we are retrieving from the primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {                                    
                IntegerSerializerDeserializer.INSTANCE, // we will use this as payload in secondary index
                UTF8StringSerializerDeserializer.INSTANCE, // we will use this ask key in secondary index
                IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE
                });
    	
        // create factories and providers for B-Tree(s)
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
        IBufferCacheProvider bufferCacheProvider = BufferCacheProvider.INSTANCE;
        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IFileMappingProviderProvider fileMappingProviderProvider = FileMappingProviderProvider.INSTANCE;
    	        
        // use a disk-order scan to read primary index    	
        BTreeDiskOrderScanOperatorDescriptor btreeScanOp = new BTreeDiskOrderScanOperatorDescriptor(spec, recDesc, bufferCacheProvider, btreeRegistryProvider, options.primaryBtreeName, fileMappingProviderProvider, interiorFrameFactory, leafFrameFactory, recDesc.getFields().length);		
		PartitionConstraint scanPartitionConstraint = createPartitionConstraint(splitNCs);
		btreeScanOp.setPartitionConstraint(scanPartitionConstraint);
		
        // sort the tuples as preparation for bulk load into secondary index
        // fields to sort on
        int[] sortFields = { 1, 0 };
        // comparators for sort fields
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
        comparatorFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, options.sbSize, sortFields, comparatorFactories, recDesc);
        PartitionConstraint sorterConstraint = createPartitionConstraint(splitNCs);
        sorter.setPartitionConstraint(sorterConstraint);
        
        // tuples to be put into B-Tree shall have 2 fields
        int fieldCount = 2;
        // the B-Tree expects its keyfields to be at the front of its input tuple 
        int[] fieldPermutation = { 1, 0 };
        BTreeBulkLoadOperatorDescriptor btreeBulkLoad = new BTreeBulkLoadOperatorDescriptor(spec, 
        		bufferCacheProvider, btreeRegistryProvider, options.btreeName, fileMappingProviderProvider, interiorFrameFactory,
                leafFrameFactory, fieldCount, comparatorFactories, fieldPermutation, 0.7f);
        PartitionConstraint bulkLoadConstraint = createPartitionConstraint(splitNCs);
        btreeBulkLoad.setPartitionConstraint(bulkLoadConstraint);        
        
        // connect the ops
               
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeScanOp, 0, sorter, 0);
                      
        //spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        
        spec.addRoot(btreeBulkLoad);
    	    	
    	return spec;
    }
    
    private static PartitionConstraint createPartitionConstraint(String[] splitNCs) {
    	LocationConstraint[] lConstraints = new LocationConstraint[splitNCs.length];
        for (int i = 0; i < splitNCs.length; ++i) {
            lConstraints[i] = new AbsoluteLocationConstraint(splitNCs[i]);
        }
        return new ExplicitPartitionConstraint(lConstraints);
    }
}