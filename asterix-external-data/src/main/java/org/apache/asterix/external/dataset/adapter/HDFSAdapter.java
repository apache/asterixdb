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
package org.apache.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.indexing.input.GenericFileAwareRecordReader;
import org.apache.asterix.external.indexing.input.GenericRecordReader;
import org.apache.asterix.external.indexing.input.TextualDataReader;
import org.apache.asterix.external.indexing.input.TextualFullScanDataReader;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Provides functionality for fetching external data stored in an HDFS instance.
 */

public class HDFSAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;

    private transient String[] readSchedule;
    private transient boolean executed[];
    private transient InputSplit[] inputSplits;
    private transient JobConf conf;
    private transient String nodeName;
    private transient List<ExternalFile> files;
    private transient Map<String, String> configuration;

    public HDFSAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits, JobConf conf,
            String nodeName, ITupleParserFactory parserFactory, IHyracksTaskContext ctx,
            Map<String, String> configuration, List<ExternalFile> files) throws HyracksDataException {
        super(parserFactory, atype, ctx);
        this.readSchedule = readSchedule;
        this.executed = executed;
        this.inputSplits = inputSplits;
        this.conf = conf;
        this.nodeName = nodeName;
        this.files = files;
        this.configuration = configuration;
    }

    /*
     * The method below was modified to take care of the following
     * 1. when target files are not null, it generates a file aware input stream that validate against the files
     * 2. if the data is binary, it returns a generic reader
     */
    @Override
    public InputStream getInputStream(int partition) throws IOException {
        if ((conf.getInputFormat() instanceof TextInputFormat
                || conf.getInputFormat() instanceof SequenceFileInputFormat)
                && (AsterixTupleParserFactory.FORMAT_ADM
                        .equalsIgnoreCase(configuration.get(AsterixTupleParserFactory.KEY_FORMAT))
                        || AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT
                                .equalsIgnoreCase(configuration.get(AsterixTupleParserFactory.KEY_FORMAT)))) {
            if (files != null) {
                return new TextualDataReader(inputSplits, readSchedule, nodeName, conf, executed, files);
            } else {
                return new TextualFullScanDataReader(executed, inputSplits, readSchedule, nodeName, conf);
            }
        } else {
            if (files != null) {
                return new GenericFileAwareRecordReader(inputSplits, readSchedule, nodeName, conf, executed, files);
            } else {
                return new GenericRecordReader(inputSplits, readSchedule, nodeName, conf, executed);
            }
        }
    }

}
