package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public interface IFileMappingProviderProvider extends Serializable {
	public IFileMappingProvider getFileMappingProvider();
}
