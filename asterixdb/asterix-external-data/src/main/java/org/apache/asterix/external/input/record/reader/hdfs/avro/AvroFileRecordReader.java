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
package org.apache.asterix.external.input.record.reader.hdfs.avro;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.input.record.reader.hdfs.AbstractHDFSRecordReader;
import org.apache.asterix.external.input.record.reader.hdfs.EmptyRecordReader;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;

public class AvroFileRecordReader<T> extends AbstractHDFSRecordReader<Void, T> {

    private AvroWrapper<T> key;
    private NullWritable value;
    private final IRawRecord<T> record;
    private RecordReader<AvroWrapper<T>, NullWritable> reader;
    private final IExternalFilterValueEmbedder valueEmbedder;
    private boolean isCompressed = false;
    private DataFileStream<T> dataFileStream;
    private FileSystem fs;
    private InputStream in;

    public AvroFileRecordReader(boolean[] read, InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, IExternalDataRuntimeContext context, UserGroupInformation ugi) {
        super(read, inputSplits, readSchedule, nodeName, conf, ugi);
        reader = new EmptyRecordReader<>();
        record = new GenericRecord<>();
        valueEmbedder = context.getValueEmbedder();
    }

    @Override
    protected boolean onNextInputSplit() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setRecordReader(int splitIndex) throws IOException {
        try {
            String filePath = getPath(inputSplits[splitIndex]);
            valueEmbedder.setPath(filePath);
            if (StringUtils.endsWithIgnoreCase(filePath, ".gz") || StringUtils.endsWithIgnoreCase(filePath, ".gzip")) {
                isCompressed = true;
                fs = ugi == null ? FileSystem.get(conf)
                        : ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(conf));
                in = new GZIPInputStream(fs.open(new Path(filePath)));
                GenericDatumReader<T> datumReader = new GenericDatumReader<>();
                dataFileStream = new DataFileStream<>(in, datumReader);
            } else {
                isCompressed = false;
                reader = (RecordReader<AvroWrapper<T>, NullWritable>) (ugi == null
                        ? inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL)
                        : ugi.doAs((PrivilegedExceptionAction<?>) () -> inputFormat
                                .getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL)));
                if (key == null) {
                    key = reader.createKey();
                    value = reader.createValue();
                }
            }
        } catch (InterruptedException ex) {
            throw HyracksDataException.create(ex);
        } catch (InvalidAvroMagicException ex) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                    ExceptionUtils.getMessageOrToString(ex));
        }
    }

    @Override
    protected void closeRecordReader() throws IOException {
        if (isCompressed) {
            dataFileStream.close();
            in.close();
            fs.close();
        } else {
            reader.close();
        }
    }

    @Override
    public void close() throws IOException {
        if (isCompressed) {
            dataFileStream.close();
            in.close();
            fs.close();
        } else {
            reader.close();
        }
    }

    @Override
    protected boolean readerHasNext() throws IOException {
        if (isCompressed) {
            return dataFileStream.hasNext();
        } else {
            return reader.next(key, value);
        }
    }

    @Override
    public IRawRecord<T> next() throws IOException {
        if (isCompressed) {
            record.set(dataFileStream.next());
        } else {
            record.set(key.datum());
        }
        return record;
    }

    private String getPath(InputSplit split) {
        if (split instanceof FileSplit) {
            return ((FileSplit) split).getPath().toString();
        } else {
            return split.toString();
        }
    }
}
