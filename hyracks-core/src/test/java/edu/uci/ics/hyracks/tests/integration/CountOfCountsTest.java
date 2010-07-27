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
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.coreops.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.InMemorySortOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.aggregators.SumStringGroupAggregator;
import edu.uci.ics.hyracks.coreops.data.StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.coreops.data.StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.coreops.data.StringComparatorFactory;
import edu.uci.ics.hyracks.coreops.data.StringSerializerDeserializer;
import edu.uci.ics.hyracks.coreops.file.CSVFileScanOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.file.FileSplit;
import edu.uci.ics.hyracks.coreops.group.PreclusteredGroupOperatorDescriptor;

public class CountOfCountsTest extends AbstractIntegrationTest {
    @Test
    public void countOfCountsSingleNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] {
            new FileSplit(NC1_ID, new File("data/words.txt"))
        };
        RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE
        });

        CSVFileScanOperatorDescriptor csvScanner = new CSVFileScanOperatorDescriptor(spec, splits, desc);
        PartitionConstraint csvPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        csvScanner.setPartitionConstraint(csvPartitionConstraint);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] {
            0
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc);
        PartitionConstraint sorterPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        sorter.setPartitionConstraint(sorterPartitionConstraint);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE
        });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            0
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(0), desc2);
        PartitionConstraint groupPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        group.setPartitionConstraint(groupPartitionConstraint);

        InMemorySortOperatorDescriptor sorter2 = new InMemorySortOperatorDescriptor(spec, new int[] {
            1
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc2);
        PartitionConstraint sorterPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        sorter2.setPartitionConstraint(sorterPartitionConstraint2);

        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            1
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(1), desc2);
        PartitionConstraint groupPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        group2.setPartitionConstraint(groupPartitionConstraint2);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                0
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                1
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void countOfCountsMultiNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] {
            new FileSplit(NC1_ID, new File("data/words.txt"))
        };
        RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE
        });

        CSVFileScanOperatorDescriptor csvScanner = new CSVFileScanOperatorDescriptor(spec, splits, desc);
        PartitionConstraint csvPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        csvScanner.setPartitionConstraint(csvPartitionConstraint);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] {
            0
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc);
        PartitionConstraint sorterPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID),
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID)
        });
        sorter.setPartitionConstraint(sorterPartitionConstraint);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE
        });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            0
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(0), desc2);
        PartitionConstraint groupPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID),
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID)
        });
        group.setPartitionConstraint(groupPartitionConstraint);

        InMemorySortOperatorDescriptor sorter2 = new InMemorySortOperatorDescriptor(spec, new int[] {
            1
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc2);
        PartitionConstraint sorterPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        sorter2.setPartitionConstraint(sorterPartitionConstraint2);

        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            1
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(1), desc2);
        PartitionConstraint groupPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        group2.setPartitionConstraint(groupPartitionConstraint2);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                0
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                1
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void countOfCountsExternalSortMultiNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] {
            new FileSplit(NC1_ID, new File("data/words.txt"))
        };
        RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE
        });

        CSVFileScanOperatorDescriptor csvScanner = new CSVFileScanOperatorDescriptor(spec, splits, desc);
        PartitionConstraint csvPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        csvScanner.setPartitionConstraint(csvPartitionConstraint);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 3, new int[] {
            0
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc);
        PartitionConstraint sorterPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID),
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID)
        });
        sorter.setPartitionConstraint(sorterPartitionConstraint);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
            StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE
        });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            0
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(0), desc2);
        PartitionConstraint groupPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID),
            new AbsoluteLocationConstraint(NC1_ID),
            new AbsoluteLocationConstraint(NC2_ID)
        });
        group.setPartitionConstraint(groupPartitionConstraint);

        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 3, new int[] {
            1
        }, new IBinaryComparatorFactory[] {
            StringBinaryComparatorFactory.INSTANCE
        }, desc2);
        PartitionConstraint sorterPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        sorter2.setPartitionConstraint(sorterPartitionConstraint2);

        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] {
            1
        }, new IComparatorFactory[] {
            StringComparatorFactory.INSTANCE
        }, new SumStringGroupAggregator(1), desc2);
        PartitionConstraint groupPartitionConstraint2 = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID)
        });
        group2.setPartitionConstraint(groupPartitionConstraint2);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
            new AbsoluteLocationConstraint(NC1_ID)
        });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                0
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNHashPartitioningConnectorDescriptor(spec,
            new FieldHashPartitionComputerFactory(new int[] {
                1
            }, new IBinaryHashFunctionFactory[] {
                StringBinaryHashFunctionFactory.INSTANCE
            }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}