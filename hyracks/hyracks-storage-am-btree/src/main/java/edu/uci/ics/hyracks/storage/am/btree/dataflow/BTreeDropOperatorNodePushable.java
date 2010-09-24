package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.File;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.file.FileManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public class BTreeDropOperatorNodePushable implements IOperatorNodePushable {
	
	private String btreeFileName;
	private IBTreeRegistryProvider btreeRegistryProvider;
	private IBufferCacheProvider bufferCacheProvider;
	private IFileMappingProvider fileMappingProvider;
	
	public BTreeDropOperatorNodePushable(IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider, String btreeFileName, IFileMappingProvider fileMappingProvider) {
		this.btreeFileName = btreeFileName;
		this.fileMappingProvider = fileMappingProvider;
		this.bufferCacheProvider = bufferCacheProvider;
		this.btreeRegistryProvider = btreeRegistryProvider;
	}

	@Override
	public void deinitialize() throws HyracksDataException {		
	}
	
	@Override
	public int getInputArity() {
		return 0;
	}
	
	@Override
	public IFrameWriter getInputFrameWriter(int index) {
		return null;
	}

	@Override
	public void initialize() throws HyracksDataException {
		
		BTreeRegistry btreeRegistry = btreeRegistryProvider.getBTreeRegistry();		
		FileManager fileManager = bufferCacheProvider.getFileManager();	
		
		String ncDataPath = System.getProperty("NodeControllerDataPath");       
        String fileName = ncDataPath + btreeFileName;
		
        int btreeFileId = fileMappingProvider.mapNameToFileId(fileName, false);        
        
		// unregister btree instance            
		btreeRegistry.lock();
		try {
			btreeRegistry.unregister(btreeFileId);		
		} finally {
			btreeRegistry.unlock();
		}

		// unregister file
		fileManager.unregisterFile(btreeFileId);				
                        
        File f = new File(fileName);
        if (f.exists()) {
			f.delete();
		}       
	}

	@Override
	public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {				
	}		
}
