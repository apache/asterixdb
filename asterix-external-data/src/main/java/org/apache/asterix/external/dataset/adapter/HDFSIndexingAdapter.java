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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.indexing.input.GenericFileAwareRecordReader;
import edu.uci.ics.asterix.external.indexing.input.RCFileDataReader;
import edu.uci.ics.asterix.external.indexing.input.TextualDataReader;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

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
