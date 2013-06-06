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

import java.util.Properties;

import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.serde.lazy.LazySerDe;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

@SuppressWarnings("deprecation")
public class HiveKeyValueParserFactory<K, V> implements IKeyValueParserFactory<K, V> {
    private static final long serialVersionUID = 1L;
    private final String serDeClass;
    private final String outputSerDeClass = LazySerDe.class.getName();;
    private final Properties tbl;
    private final ConfFactory confFactory;
    private final int[] outputColumnsOffset;

    public HiveKeyValueParserFactory(PartitionDesc desc, JobConf conf, int[] outputColumnsOffset)
            throws HyracksDataException {
        this.tbl = desc.getProperties();
        this.serDeClass = (String) tbl.getProperty("serialization.lib");
        this.outputColumnsOffset = outputColumnsOffset;
        this.confFactory = new ConfFactory(conf);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public IKeyValueParser<K, V> createKeyValueParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new HiveKeyValueParser(serDeClass, outputSerDeClass, tbl, confFactory.getConf(), ctx,
                outputColumnsOffset);
    }

}
