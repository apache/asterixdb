package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.am.btree.dataflow.IFileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public class FileMappingProviderProvider implements IFileMappingProviderProvider {
	private static final long serialVersionUID = 1L;
	
	public static final IFileMappingProviderProvider INSTANCE = new FileMappingProviderProvider();
	
	@Override
	public IFileMappingProvider getFileMappingProvider() {
		return RuntimeContext.getInstance().getFileMappingProvider();
	}
}
