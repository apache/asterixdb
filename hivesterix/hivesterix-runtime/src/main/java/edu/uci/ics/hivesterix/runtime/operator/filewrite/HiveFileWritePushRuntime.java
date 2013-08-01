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
package edu.uci.ics.hivesterix.runtime.operator.filewrite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hivesterix.serde.lazy.LazyColumnar;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyColumnarObjectInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

@SuppressWarnings("deprecation")
public class HiveFileWritePushRuntime implements IPushRuntime {
    private final static Logger LOGGER = Logger.getLogger(HiveFileWritePushRuntime.class.getName());

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
            createTempDir();
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

    private void createTempDir() throws IOException {
        FileSinkDesc fdesc = fsOp.getConf();
        String tempDir = fdesc.getDirName();
        if (tempDir != null) {
            Path tempPath = Utilities.toTempPath(new Path(tempDir));
            FileSystem fs = tempPath.getFileSystem(conf);
            if (!fs.exists(tempPath)) {
                try {
                    fs.mkdirs(tempPath);
                    ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs, tempPath);
                } catch (IOException e) {
                    //if the dir already exists, that should be fine; so log a warning msg
                    LOGGER.warning("create tmp result directory fails.");
                }
            }
        }
    }

}
