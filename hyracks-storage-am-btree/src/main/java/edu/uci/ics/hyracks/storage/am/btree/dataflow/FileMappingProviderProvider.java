package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public class FileMappingProviderProvider implements IFileMappingProviderProvider {
	private static final long serialVersionUID = 1L;
	
	private static IFileMappingProvider fileMappingProvider = null;
	
	@Override
	public IFileMappingProvider getFileMappingProvider() {
		if(fileMappingProvider == null) {
			fileMappingProvider = new FileMappingProvider();
		}		
		return fileMappingProvider;
	}
}
