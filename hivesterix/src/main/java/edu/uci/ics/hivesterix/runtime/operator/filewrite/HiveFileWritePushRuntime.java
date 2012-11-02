package edu.uci.ics.hivesterix.runtime.operator.filewrite;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.serde.lazy.LazyColumnar;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyColumnarObjectInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class HiveFileWritePushRuntime implements IPushRuntime {

    /**
     * frame tuple accessor to access byte buffer
     */
    private final FrameTupleAccessor accessor;

    /**
     * input object inspector
     */
    private final ObjectInspector inputInspector;

    /**
     * cachedInput
     */
    private final LazyColumnar cachedInput;

    /**
     * File sink operator of Hive
     */
    private final FileSinkDesc fileSink;

    /**
     * job configuration, which contain name node and other configuration
     * information
     */
    private JobConf conf;

    /**
     * input object inspector
     */
    private final Schema inputSchema;

    /**
     * a copy of hive schema representation
     */
    private RowSchema rowSchema;

    /**
     * the Hive file sink operator
     */
    private FileSinkOperator fsOp;

    /**
     * cached tuple object reference
     */
    private FrameTupleReference tuple = new FrameTupleReference();

    /**
     * @param spec
     * @param fsProvider
     */
    public HiveFileWritePushRuntime(IHyracksTaskContext context, RecordDescriptor inputRecordDesc, JobConf job,
            FileSinkDesc fs, RowSchema schema, Schema oi) {
        fileSink = fs;
        fileSink.setGatherStats(false);

        rowSchema = schema;
        conf = job;
        inputSchema = oi;

        accessor = new FrameTupleAccessor(context.getFrameSize(), inputRecordDesc);
        inputInspector = inputSchema.toObjectInspector();
        cachedInput = new LazyColumnar((LazyColumnarObjectInspector) inputInspector);
    }

    @Override
    public void open() throws HyracksDataException {
        fsOp = (FileSinkOperator) OperatorFactory.get(fileSink, rowSchema);
        fsOp.setChildOperators(null);
        fsOp.setParentOperators(null);
        conf.setClassLoader(this.getClass().getClassLoader());

        ObjectInspector[] inspectors = new ObjectInspector[1];
        inspectors[0] = inputInspector;
        try {
            fsOp.initialize(conf, inspectors);
            fsOp.setExecContext(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int n = accessor.getTupleCount();
        try {
            for (int i = 0; i < n; ++i) {
                tuple.reset(accessor, i);
                cachedInput.init(tuple);
                fsOp.process(cachedInput, 0);
            }
        } catch (HiveException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            fsOp.closeOp(false);
        } catch (HiveException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        throw new IllegalStateException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
    }

    @Override
    public void fail() throws HyracksDataException {

    }

}
