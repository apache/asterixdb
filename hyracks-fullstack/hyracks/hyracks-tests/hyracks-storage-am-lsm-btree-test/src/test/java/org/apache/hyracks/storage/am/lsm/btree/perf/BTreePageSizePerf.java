/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.btree.perf;

import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;

public class BTreePageSizePerf {
    public static void main(String[] args) throws Exception {
        // Disable logging so we can better see the output times.
        Enumeration<String> loggers = LogManager.getLogManager().getLoggerNames();
        while (loggers.hasMoreElements()) {
            String loggerName = loggers.nextElement();
            Logger logger = LogManager.getLogManager().getLogger(loggerName);
            logger.setLevel(Level.OFF);
        }

        int numTuples = 1000000;
        int batchSize = 10000;
        int numBatches = numTuples / batchSize;

        ISerializerDeserializer[] fieldSerdes =
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes, 30);

        IBinaryComparatorFactory[] cmpFactories =
                SerdeUtils.serdesToComparatorFactories(fieldSerdes, fieldSerdes.length);

        runExperiment(numBatches, batchSize, 1024, 100000, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 2048, 100000, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 4096, 25000, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 8192, 12500, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 16384, 6250, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 32768, 3125, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 65536, 1564, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 131072, 782, fieldSerdes, cmpFactories, typeTraits);
        runExperiment(numBatches, batchSize, 262144, 391, fieldSerdes, cmpFactories, typeTraits);
    }

    private static void runExperiment(int numBatches, int batchSize, int pageSize, int numPages,
            ISerializerDeserializer[] fieldSerdes, IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] typeTraits)
            throws Exception {
        System.out.println("PAGE SIZE: " + pageSize);
        System.out.println("NUM PAGES: " + numPages);
        System.out.println("MEMORY: " + (pageSize * numPages));
        int repeats = 5;
        long[] times = new long[repeats];
        //BTreeRunner runner = new BTreeRunner(numTuples, pageSize, numPages, typeTraits, cmp);
        InMemoryBTreeRunner runner = new InMemoryBTreeRunner(numBatches, pageSize, numPages, typeTraits, cmpFactories);
        runner.init();
        int numThreads = 1;
        for (int i = 0; i < repeats; i++) {
            DataGenThread dataGen =
                    new DataGenThread(numThreads, numBatches, batchSize, fieldSerdes, 30, 50, 10, false);
            dataGen.start();
            times[i] = runner.runExperiment(dataGen, numThreads);
            System.out.println("TIME " + i + ": " + times[i] + "ms");
        }
        runner.deinit();
        long avgTime = 0;
        for (int i = 0; i < repeats; i++) {
            avgTime += times[i];
        }
        avgTime /= repeats;
        System.out.println("AVG TIME: " + avgTime + "ms");
        System.out.println("-------------------------------");
    }
}
