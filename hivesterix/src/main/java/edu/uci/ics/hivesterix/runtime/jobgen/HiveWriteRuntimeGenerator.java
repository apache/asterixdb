package edu.uci.ics.hivesterix.runtime.jobgen;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.runtime.config.ConfUtil;
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
