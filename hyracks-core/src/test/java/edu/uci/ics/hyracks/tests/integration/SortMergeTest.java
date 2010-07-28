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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.coreops.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.InMemorySortOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.data.StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.coreops.data.StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.coreops.data.StringSerializerDeserializer;
import edu.uci.ics.hyracks.coreops.file.CSVFileScanOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.file.FileSplit;

public class SortMergeTest extends AbstractIntegrationTest {
    @Test
    public void sortMergeTest01() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] {
            new FileSplit(NC1_ID, new File("data/tpch0.001/orders-part1.tbl")),
            new FileSplit(NC2_ID, new File("data/tpch0.001/orders-part2.tbl"))
        };
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE,
            StringSerializerDeserializer.INSTANCE
        });

        CSVFileScanOperatorDescriptor ordScanner = new CSVFileScanOperatorDescriptor(spec, ordersSplits, ordersDesc,
            '|', "'\"");
        PartitionConstraint ordersPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        ordScanner.setPartitionConstraint(ordersPartitionConstraint);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] {
            1
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, ordersDesc);
        PartitionConstraint sortersPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        sorter.setPartitionConstraint(sortersPartitionConstraint);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        printer.setPartitionConstraint(printerPartitionConstraint);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(new MToNHashPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
            new int[] {
                1
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }), new int[] {
            1
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }), sorter, 0, printer, 0);

        runTest(spec);
    }
}