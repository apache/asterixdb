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

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import org.apache.asterix.external.adapter.factory.HDFSAdapterFactory;
import org.apache.asterix.external.indexing.input.GenericFileAwareRecordReader;
import org.apache.asterix.external.indexing.input.RCFileDataReader;
import org.apache.asterix.external.indexing.input.TextualDataReader;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class HDFSIndexingAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;
    private transient String[] readSchedule;
    private transient boolean executed[];
    private transient InputSplit[] inputSplits;
    private transient JobConf conf;
    private final List<ExternalFile> files;
    private transient String nodeName;
    // file input-format <text, seq, rc>
    private String inputFormat;
    // content format <adm, delimited-text, binary>
    private String format;

    public HDFSIndexingAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits,
            JobConf conf, AlgebricksPartitionConstraint clusterLocations, List<ExternalFile> files,
            ITupleParserFactory parserFactory, IHyracksTaskContext ctx, String nodeName, String inputFormat,
            String format) throws IOException {
        super(parserFactory, atype, ctx);
        this.nodeName = nodeName;
        this.readSchedule = readSchedule;
        this.executed = executed;
        this.inputSplits = inputSplits;
        this.conf = conf;
        this.files = files;
        this.inputFormat = inputFormat;
        this.format = format;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        if (inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_RC)) {
            return new RCFileDataReader(inputSplits, readSchedule, nodeName, conf, executed, files);
        } else if (format.equals(AsterixTupleParserFactory.FORMAT_ADM)
                || format.equals(AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT)) {
            return new TextualDataReader(inputSplits, readSchedule, nodeName, conf, executed, files);
        } else {
            return new GenericFileAwareRecordReader(inputSplits, readSchedule, nodeName, conf, executed, files);
        }
    }
}
