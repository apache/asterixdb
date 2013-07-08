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

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hivesterix.runtime.operator.filewrite.HivePushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

@SuppressWarnings("deprecation")
public class HiveWriteRuntimeGenerator {
    private FileSinkOperator fileSink;

    private Schema inputSchema;

    public HiveWriteRuntimeGenerator(FileSinkOperator fsOp, Schema oi) {
        fileSink = fsOp;
        inputSchema = oi;
    }

    /**
     * get the write runtime
     * 
     * @param inputDesc
     * @return
     */
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriterRuntime(RecordDescriptor inputDesc) {
        JobConf conf = ConfUtil.getJobConf();
        IPushRuntimeFactory factory = new HivePushRuntimeFactory(inputDesc, conf, fileSink, inputSchema);
        Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> pair = new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(
                factory, null);
        return pair;
    }
}
