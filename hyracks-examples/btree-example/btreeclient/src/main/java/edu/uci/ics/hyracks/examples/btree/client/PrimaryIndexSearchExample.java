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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.BTreeRegistryProvider;
import edu.uci.ics.hyracks.examples.btree.helper.BufferCacheProvider;
import edu.uci.ics.hyracks.examples.btree.helper.FileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IFileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.ITupleReferenceFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.NullTupleReferenceFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;

// This example will perform an ordered scan on the primary index
// i.e. a range-search for [-infinity, +infinity]

public class PrimaryIndexSearchExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;
        
        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;
                
        @Option(name = "-btreename", usage = "B-Tree file name to search", required = true)
        public String btreeName;               
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
    	
        // create factories and providers for B-Tree
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();        
        IBufferCacheProvider bufferCacheProvider = BufferCacheProvider.INSTANCE;
        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IFileMappingProviderProvider fileMappingProviderProvider = FileMappingProviderProvider.INSTANCE;
    	
    	// schema of tuples coming out of primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {                
        		IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE,    
                IntegerSerializerDeserializer.INSTANCE,                           
                UTF8StringSerializerDeserializer.INSTANCE,
                });
        
        // comparators for btree
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;     
        
        // build search key factories
    	ITupleReferenceFactory[] searchKeys = new ITupleReferenceFactory[2]; 
    	searchKeys[0] = new NullTupleReferenceFactory(); // equivalent to -infinity
    	searchKeys[1] = new NullTupleReferenceFactory(); // equivalent to +infinity
        
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, recDesc, bufferCacheProvider, btreeRegistryProvider, options.btreeName, fileMappingProviderProvider, interiorFrameFactory, leafFrameFactory, recDesc.getFields().length, comparatorFactories, true, searchKeys, comparatorFactories.length);
        PartitionConstraint btreeSearchConstraint = createPartitionConstraint(splitNCs);
        btreeSearchOp.setPartitionConstraint(btreeSearchConstraint);
        
        // have each node print the results of its respective B-Tree
        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerConstraint = createPartitionConstraint(splitNCs);
        printer.setPartitionConstraint(printerConstraint);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeSearchOp, 0, printer, 0);
        
        spec.addRoot(printer);
    	    	
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