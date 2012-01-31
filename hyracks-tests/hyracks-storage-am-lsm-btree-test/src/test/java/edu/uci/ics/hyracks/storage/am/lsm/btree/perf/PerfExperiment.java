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

package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;

public class PerfExperiment {
    public static void main(String[] args) throws Exception {
        // Disable logging so we can better see the output times.
        Enumeration<String> loggers = LogManager.getLogManager().getLoggerNames();
        while(loggers.hasMoreElements()) {
            String loggerName = loggers.nextElement();
            Logger logger = LogManager.getLogManager().getLogger(loggerName);
            logger.setLevel(Level.OFF);
        }
        
        int numTuples = 100000; // 100K
        //int numTuples = 1000000; // 1M
        //int numTuples = 2000000; // 2M
        //int numTuples = 3000000; // 3M
        //int numTuples = 10000000; // 10M
        //int numTuples = 20000000; // 20M
        //int numTuples = 30000000; // 30M
        //int numTuples = 40000000; // 40M
        //int numTuples = 60000000; // 60M
        //int numTuples = 100000000; // 100M
        //int numTuples = 200000000; // 200M
        int batchSize = 10000;
        int numBatches = numTuples / batchSize;
        
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes, 30);
        
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, fieldSerdes.length);
        
        //int repeats = 1000;
        int repeats = 1;
        long[] times = new long[repeats];

        int numThreads = 2;
        for (int i = 0; i < repeats; i++) {
            //ConcurrentSkipListRunner runner = new ConcurrentSkipListRunner(numBatches, batchSize, tupleSize, typeTraits, cmp);
            InMemoryBTreeRunner runner = new InMemoryBTreeRunner(numBatches, 8192, 100000, typeTraits, cmpFactories);
            //BTreeBulkLoadRunner runner = new BTreeBulkLoadRunner(numBatches, 8192, 100000, typeTraits, cmp, 1.0f);
        	//BTreeRunner runner = new BTreeRunner(numBatches, 8192, 100000, typeTraits, cmp);
        	//String btreeName = "071211";
        	//BTreeSearchRunner runner = new BTreeSearchRunner(btreeName, 10, numBatches, 8192, 25000, typeTraits, cmp);
        	//LSMTreeRunner runner = new LSMTreeRunner(numBatches, 8192, 100, 8192, 250, typeTraits, cmp);
        	//LSMTreeSearchRunner runner = new LSMTreeSearchRunner(100000, numBatches, 8192, 24750, 8192, 250, typeTraits, cmp); 
            DataGenThread dataGen = new DataGenThread(numBatches, batchSize, 10, numThreads, fieldSerdes, 30, 50, false);
            dataGen.start();
            runner.reset();
            times[i] = runner.runExperiment(dataGen, numThreads);
            System.out.println("TIME " + i + ": " + times[i] + "ms");
            runner.deinit();
        }
        long avgTime = 0;
        for (int i = 0; i < repeats; i++) {
            avgTime += times[i];
        }
        avgTime /= repeats;
        System.out.println("AVG TIME: " + avgTime + "ms");
    }
}
