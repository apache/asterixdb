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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;

/**
 * Represents the base class that is required to be extended by every
 * implementation of the IDatasourceAdapter interface.
 */
public abstract class AbstractDatasourceAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = -3510610289692452466L;

    protected Map<String, String> configuration;

    protected AlgebricksPartitionConstraint partitionConstraint;

    protected IAType atype;

    protected IHyracksTaskContext ctx;

    protected IDataParser dataParser;

    protected static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();

    protected static final HashMap<String, String> formatToParserMap = new HashMap<String, String>();

    protected static final HashMap<String, String> formatToManagedParserMap = new HashMap<String, String>();

    protected AdapterDataFlowType dataFlowType;

    protected AdapterType adapterType;

    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);

        formatToParserMap.put("delimited-text", "edu.uci.ics.asterix.external.data.parser.DelimitedDataStreamParser");
        formatToParserMap.put("adm", "edu.uci.ics.asterix.external.data.parser.ADMStreamParser");

        formatToManagedParserMap.put("delimited-text",
                "edu.uci.ics.asterix.external.data.parser.ManagedDelimitedDataStreamParser");
        formatToManagedParserMap.put("adm", "edu.uci.ics.asterix.external.data.parser.ManagedAdmStreamParser");

    }

    public static final String KEY_FORMAT = "format";
    public static final String KEY_PARSER = "parser";

    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_ADM = "adm";

    abstract public void initialize(IHyracksTaskContext ctx) throws Exception;

    abstract public void configure(Map<String, String> arguments, IAType atype) throws Exception;

    abstract public AdapterDataFlowType getAdapterDataFlowType();

    abstract public AdapterType getAdapterType();

    public AlgebricksPartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    public void setAdapterProperty(String property, String value) {
        configuration.put(property, value);
    }

    public IDataParser getParser() {
        return dataParser;
    }

    public void setParser(IDataParser dataParser) {
        this.dataParser = dataParser;
    }

    public String getAdapterProperty(String attribute) {
        return configuration.get(attribute);
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

}
