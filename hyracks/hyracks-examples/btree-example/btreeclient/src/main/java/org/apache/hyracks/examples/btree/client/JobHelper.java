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

package edu.uci.ics.hyracks.examples.btree.client;

import java.io.File;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class JobHelper {
    public static IFileSplitProvider createFileSplitProvider(String[] splitNCs, String btreeFileName) {
        FileSplit[] fileSplits = new FileSplit[splitNCs.length];
        for (int i = 0; i < splitNCs.length; ++i) {
            String fileName = btreeFileName + "." + splitNCs[i];
            fileSplits[i] = new FileSplit(splitNCs[i], new FileReference(new File(fileName)));
        }
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        return splitProvider;
    }

    public static void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, String[] splitNCs) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, splitNCs);
    }
}
