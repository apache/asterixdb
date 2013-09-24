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
package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hivesterix.serde.parser.IHiveParser;
import edu.uci.ics.hivesterix.serde.parser.TextToBinaryTupleParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;

@SuppressWarnings("deprecation")
public class HiveKeyValueParser<K, V> implements IKeyValueParser<K, V> {
    /**
     * the columns to output: projection is pushed into this scan
     */
    private int[] outputColumnsOffset;

    /**
     * serialization/de-serialization object
     */
    private SerDe serDe;

    /**
     * the input row object inspector
     */
    private StructObjectInspector structInspector;

    /**
     * the hadoop job conf
     */
    private JobConf job;

    /**
     * Hyrax context to control resource allocation
     */
    private final IHyracksTaskContext ctx;

    /**
     * lazy serde: format flow in between operators
     */
    private final SerDe outputSerDe;

    /**
     * the parser from hive data to binary data
     */
    private IHiveParser parser;

    /**
     * the buffer for buffering output data
     */
    private ByteBuffer buffer;

    /**
     * the frame tuple appender
     */
    private FrameTupleAppender appender;

    /**
     * the array tuple builder
     */
    private ArrayTupleBuilder tb;

    /**
     * the field references of all fields
     */
    private List<? extends StructField> fieldRefs;

    /**
     * output fields
     */
    private Object[] outputFields;

    /**
     * output field references
     */
    private StructField[] outputFieldRefs;

    public HiveKeyValueParser(String serDeClass, String outputSerDeClass, Properties tbl, JobConf conf,
            final IHyracksTaskContext ctx, int[] outputColumnsOffset) throws HyracksDataException {
        try {
            job = conf;
            // initialize the input serde
            serDe = (SerDe) ReflectionUtils.newInstance(Class.forName(serDeClass), job);
            serDe.initialize(job, tbl);
            // initialize the output serde
            outputSerDe = (SerDe) ReflectionUtils.newInstance(Class.forName(outputSerDeClass), job);
            outputSerDe.initialize(job, tbl);
            // object inspector of the row
            structInspector = (StructObjectInspector) serDe.getObjectInspector();
            // hyracks context
            this.ctx = ctx;
            this.outputColumnsOffset = outputColumnsOffset;

            if (structInspector instanceof LazySimpleStructObjectInspector) {
                LazySimpleStructObjectInspector rowInspector = (LazySimpleStructObjectInspector) structInspector;
                List<? extends StructField> fieldRefs = rowInspector.getAllStructFieldRefs();
                boolean lightWeightParsable = true;
                for (StructField fieldRef : fieldRefs) {
                    Category category = fieldRef.getFieldObjectInspector().getCategory();
                    if (!(category == Category.PRIMITIVE)) {
                        lightWeightParsable = false;
                        break;
                    }
                }
                if (lightWeightParsable) {
                    parser = new TextToBinaryTupleParser(this.outputColumnsOffset, structInspector);
                }
            }

            fieldRefs = structInspector.getAllStructFieldRefs();
            int size = 0;
            for (int i = 0; i < outputColumnsOffset.length; i++) {
                if (outputColumnsOffset[i] >= 0) {
                    size++;
                }
            }

            tb = new ArrayTupleBuilder(size);
            outputFieldRefs = new StructField[size];
            outputFields = new Object[size];
            for (int i = 0; i < outputColumnsOffset.length; i++)
                if (outputColumnsOffset[i] >= 0)
                    outputFieldRefs[outputColumnsOffset[i]] = fieldRefs.get(i);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void open(IFrameWriter writer) throws HyracksDataException {
        buffer = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(buffer, true);
    }

    @Override
    public void parse(K key, V value, IFrameWriter writer, String fileString) throws HyracksDataException {
        try {
            tb.reset();
            if (parser != null) {
                Text text = (Text) value;
                parser.parse(text.getBytes(), 0, text.getLength(), tb);
            } else {
                Object row = serDe.deserialize((Writable) value);
                /**
                 * write fields to the tuple builder one by one
                 */
                int i = 0;
                for (StructField fieldRef : fieldRefs) {
                    if (outputColumnsOffset[i] >= 0)
                        outputFields[outputColumnsOffset[i]] = structInspector.getStructFieldData(row, fieldRef);
                    i++;
                }
                i = 0;
                DataOutput dos = tb.getDataOutput();
                for (Object field : outputFields) {
                    BytesWritable fieldWritable = (BytesWritable) outputSerDe.serialize(field,
                            outputFieldRefs[i].getFieldObjectInspector());
                    dos.write(fieldWritable.getBytes(), 0, fieldWritable.getSize());
                    tb.addFieldEndOffset();
                    i++;
                }
            }

            /**
             * append the tuple and flush it if necessary.
             */
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(buffer, writer);
                appender.reset(buffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close(IFrameWriter writer) throws HyracksDataException {
        /**
         * flush the residual tuples
         */
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(buffer, writer);
        }
        System.gc();
    }

}
