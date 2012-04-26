/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.external.data.parser.IDataStreamParser;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class NCFileSystemAdapter extends AbstractDatasourceAdapter implements IDatasourceReadAdapter {

    private static final long serialVersionUID = -4154256369973615710L;
    private FileSplit[] fileSplits;
    private String parserClass;

    public class Constants {
        public static final String KEY_SPLITS = "path";
        public static final String KEY_FORMAT = "format";
        public static final String KEY_PARSER = "parser";
        public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
        public static final String FORMAT_ADM = "adm";
    }

    @Override
    public void configure(Map<String, String> arguments, IAType atype) throws Exception {
        this.configuration = arguments;
        String[] splits = arguments.get(Constants.KEY_SPLITS).split(",");
        configureFileSplits(splits);
        configurePartitionConstraint();
        configureFormat();
        if (atype == null) {
            configureInputType();
        } else {
            setInputAType(atype);
        }
    }

    public IAType getAType() {
        return atype;
    }

    public void setInputAType(IAType atype) {
        this.atype = atype;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public AdapterDataFlowType getAdapterDataFlowType() {
        return AdapterDataFlowType.PULL;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public IDataParser getDataParser(int partition) throws Exception {
        FileSplit split = fileSplits[partition];
        File inputFile = split.getLocalFile().getFile();
        InputStream in;
        try {
            in = new FileInputStream(inputFile);
        } catch (FileNotFoundException e) {
            throw new HyracksDataException(e);
        }

        IDataParser dataParser = (IDataParser) Class.forName(parserClass).newInstance();
        if (dataParser instanceof IDataStreamParser) {
            ((IDataStreamParser) dataParser).setInputStream(in);
        } else {
            throw new IllegalArgumentException(" parser not compatible");
        }
        dataParser.configure(configuration);
        dataParser.initialize((ARecordType) atype, ctx);
        return dataParser;
    }

    private void configureFileSplits(String[] splits) {
        if (fileSplits == null) {
            fileSplits = new FileSplit[splits.length];
            String nodeName;
            String nodeLocalPath;
            int count = 0;
            for (String splitPath : splits) {
                nodeName = splitPath.split(":")[0];
                nodeLocalPath = splitPath.split("://")[1];
                FileSplit fileSplit = new FileSplit(nodeName, new FileReference(new File(nodeLocalPath)));
                fileSplits[count++] = fileSplit;
            }
        }
    }

    protected void configureFormat() throws Exception {
        parserClass = configuration.get(Constants.KEY_PARSER);
        if (parserClass == null) {
            if (Constants.FORMAT_DELIMITED_TEXT.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parserClass = formatToParserMap.get(FORMAT_DELIMITED_TEXT);
            } else if (Constants.FORMAT_ADM.equalsIgnoreCase(configuration.get(Constants.KEY_FORMAT))) {
                parserClass = formatToParserMap.get(Constants.FORMAT_ADM);
            } else {
                throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
            }
        }

    }

    private void configureInputType() {
        throw new UnsupportedOperationException(" Cannot resolve input type, operation not supported");
    }

    private void configurePartitionConstraint() {
        String[] locs = new String[fileSplits.length];
        for (int i = 0; i < fileSplits.length; i++) {
            locs[i] = fileSplits[i].getNodeName();
        }
        partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locs);
    }

}
