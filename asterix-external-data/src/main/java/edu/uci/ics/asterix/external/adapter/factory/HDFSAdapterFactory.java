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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSAccessByRIDAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSIndexingAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IControlledAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

/**
 * A factory class for creating an instance of HDFSAdapter
 */
@SuppressWarnings("deprecation")
public class HDFSAdapterFactory implements IGenericDatasetAdapterFactory {
	private static final long serialVersionUID = 1L;

	public static final String HDFS_ADAPTER_NAME = "hdfs";
	public static final String CLUSTER_LOCATIONS = "cluster-locations";
	public static transient String SCHEDULER = "hdfs-scheduler";

	public static final String KEY_HDFS_URL = "hdfs";
	public static final String KEY_PATH = "path";
	public static final String KEY_INPUT_FORMAT = "input-format";
	public static final String INPUT_FORMAT_TEXT = "text-input-format";
	public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";
	public static final String INPUT_FORMAT_RC = "rc-input-format";
	public static final String KEY_DELIMITER = "delimiter";
	public static final String KEY_FORMAT = "format";
	public static final String FORMAT_DELIMITED_TEXT = "delimited-text";

	private transient AlgebricksPartitionConstraint clusterLocations;
	private String[] readSchedule;
	private boolean executed[];
	private InputSplitsFactory inputSplitsFactory;
	private ConfFactory confFactory;
	private boolean setup = false;

	private static final Map<String, String> formatClassNames = initInputFormatMap();

	private static Map<String, String> initInputFormatMap() {
		Map<String, String> formatClassNames = new HashMap<String, String>();
		formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
		formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
		formatClassNames.put(INPUT_FORMAT_RC, "org.apache.hadoop.hive.ql.io.RCFileInputFormat");
		return formatClassNames;
	}

	@Override
	public IDatasourceAdapter createAdapter(Map<String, Object> configuration, IAType atype) throws Exception {
		if (!setup) {
			/** set up the factory --serializable stuff --- this if-block should be called only once for each factory instance */
			configureJobConf(configuration);
			JobConf conf = configureJobConf(configuration);
			confFactory = new ConfFactory(conf);

			clusterLocations = (AlgebricksPartitionConstraint) configuration.get(CLUSTER_LOCATIONS);
			int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;

			InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
			inputSplitsFactory = new InputSplitsFactory(inputSplits);

			Scheduler scheduler = (Scheduler) configuration.get(SCHEDULER);
			readSchedule = scheduler.getLocationConstraints(inputSplits);
			executed = new boolean[readSchedule.length];
			Arrays.fill(executed, false);

			setup = true;
		}
		JobConf conf = confFactory.getConf();
		InputSplit[] inputSplits = inputSplitsFactory.getSplits();
		HDFSAdapter hdfsAdapter = new HDFSAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations);

		//If input format is rcfile, configure parser expected format to delimeted text with 0x01 (default ) as delimiter
		if(((String)configuration.get(KEY_INPUT_FORMAT)).equals(INPUT_FORMAT_RC))
		{
			char delimeter = 0x01;
			configuration.put(KEY_FORMAT, FORMAT_DELIMITED_TEXT);
			configuration.put(KEY_DELIMITER, Character.toString(delimeter));
		}

		hdfsAdapter.configure(configuration);
		return hdfsAdapter;
	}

	@Override
	public IControlledAdapter createAccessByRIDAdapter(
			Map<String, Object> configuration, IAType atype, HashMap<Integer, String> files) throws Exception {
		Configuration conf = configureHadoopConnection(configuration);
		clusterLocations = (AlgebricksPartitionConstraint) configuration.get(CLUSTER_LOCATIONS);
		
		//Create RID record desc
		RecordDescriptor ridRecordDesc = null;

		//If input format is rcfile, configure parser expected format to delimeted text with control char 0x01 as delimiter
		if(((String)configuration.get(KEY_INPUT_FORMAT)).equals(INPUT_FORMAT_RC))
		{
			char delimeter = 0x01;
			configuration.put(KEY_FORMAT, FORMAT_DELIMITED_TEXT);
			configuration.put(KEY_DELIMITER, Character.toString(delimeter));
			ridRecordDesc = getRIDRecDesc(true, files != null);
		}
		else
		{
			ridRecordDesc = getRIDRecDesc(false, files != null);
		}
		HDFSAccessByRIDAdapter adapter = new HDFSAccessByRIDAdapter(atype, ((String)configuration.get(KEY_INPUT_FORMAT)), clusterLocations,ridRecordDesc, conf, files);
		adapter.configure(configuration);
		return adapter;
	}

	@Override
	public IDatasourceAdapter createIndexingAdapter(Map<String, Object> configuration, IAType atype, Map<String,Integer> files) throws Exception {
		if (!setup) {
			/** set up the factory --serializable stuff --- this if-block should be called only once for each factory instance */
			configureJobConf(configuration);
			JobConf conf = configureJobConf(configuration);
			confFactory = new ConfFactory(conf);

			clusterLocations = (AlgebricksPartitionConstraint) configuration.get(CLUSTER_LOCATIONS);
			int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;

			InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
			inputSplitsFactory = new InputSplitsFactory(inputSplits);

			Scheduler scheduler = (Scheduler) configuration.get(SCHEDULER);
			readSchedule = scheduler.getLocationConstraints(inputSplits);
			executed = new boolean[readSchedule.length];
			Arrays.fill(executed, false);

			setup = true;
		}
		JobConf conf = confFactory.getConf();
		InputSplit[] inputSplits = inputSplitsFactory.getSplits();
		//If input format is rcfile, configure parser expected format to delimeted text with 0x01 (default) as delimiter
		if(((String)configuration.get(KEY_INPUT_FORMAT)).equals(INPUT_FORMAT_RC))
		{
			char delimeter = 0x01;
			configuration.put(KEY_FORMAT, FORMAT_DELIMITED_TEXT);
			configuration.put(KEY_DELIMITER, Character.toString(delimeter));	
		}
		HDFSIndexingAdapter hdfsIndexingAdapter = new HDFSIndexingAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations, files);
		hdfsIndexingAdapter.configure(configuration);
		return hdfsIndexingAdapter;
	}

	@Override
	public String getName() {
		return HDFS_ADAPTER_NAME;
	}

	private JobConf configureJobConf(Map<String, Object> configuration) throws Exception {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", ((String) configuration.get(KEY_HDFS_URL)).trim());
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.setClassLoader(HDFSAdapter.class.getClassLoader());
		conf.set("mapred.input.dir", ((String) configuration.get(KEY_PATH)).trim());
		conf.set("mapred.input.format.class",
				(String) formatClassNames.get(((String) configuration.get(KEY_INPUT_FORMAT)).trim()));
		return conf;
	}

	public static Configuration configureHadoopConnection(Map<String, Object> configuration)
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", ((String) configuration.get(KEY_HDFS_URL)).trim());
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		return conf;
	}
	
	public static RecordDescriptor getRIDRecDesc(boolean isRCFile, boolean optimize){
		int numOfPrimaryKeys = 2;
		if(isRCFile)
		{
			numOfPrimaryKeys++;
		}
		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] serde = new ISerializerDeserializer[numOfPrimaryKeys];
		ITypeTraits[] tt = new ITypeTraits[numOfPrimaryKeys];
		if(optimize)
		{
			serde[0] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AINT32);
			tt[0] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT32);
		}
		else
		{
			serde[0] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.ASTRING);
			tt[0] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ASTRING);
		}
		serde[1] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AINT64);
		tt[1] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT64);
		if(isRCFile)
		{
			//we add the row number for rc-files
			serde[2] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AINT32);
			tt[2] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT32);
		}
		return new RecordDescriptor(serde, tt);
	}


}
