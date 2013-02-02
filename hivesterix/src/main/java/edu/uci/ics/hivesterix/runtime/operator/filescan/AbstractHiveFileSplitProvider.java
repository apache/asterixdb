package edu.uci.ics.hivesterix.runtime.operator.filescan;

import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public abstract class AbstractHiveFileSplitProvider implements
		IFileSplitProvider {
	private static final long serialVersionUID = 1L;

	@Override
	public FileSplit[] getFileSplits() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("deprecation")
	public abstract org.apache.hadoop.mapred.FileSplit[] getFileSplitArray();

}
