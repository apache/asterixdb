package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.File;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BTreeDropOperatorNodePushable extends AbstractOperatorNodePushable {

    private String btreeFileName;
    private IBTreeRegistryProvider btreeRegistryProvider;
    private IBufferCacheProvider bufferCacheProvider;
    private IFileMappingProviderProvider fileMappingProviderProvider;
    private IFileSplitProvider fileSplitProvider;
    private int partition;
    
    public BTreeDropOperatorNodePushable(IBufferCacheProvider bufferCacheProvider,
            IBTreeRegistryProvider btreeRegistryProvider, IFileSplitProvider fileSplitProvider, int partition,
            IFileMappingProviderProvider fileMappingProviderProvider) {
        this.fileMappingProviderProvider = fileMappingProviderProvider;
        this.bufferCacheProvider = bufferCacheProvider;
        this.btreeRegistryProvider = btreeRegistryProvider;
        this.fileSplitProvider = fileSplitProvider;
        this.partition = partition;
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
        
        File f = fileSplitProvider.getFileSplits()[partition].getLocalFile();        
        String fileName = f.getAbsolutePath();            
        
        int btreeFileId = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId(fileName, false);

        // unregister btree instance            
        btreeRegistry.lock();
        try {
            btreeRegistry.unregister(btreeFileId);
        } finally {
            btreeRegistry.unlock();
        }

        // unregister file
        fileManager.unregisterFile(btreeFileId);
        
        if (f.exists()) {
            f.delete();
        }

        // remove name to id mapping
        fileMappingProviderProvider.getFileMappingProvider().unmapName(fileName);
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
    }
}
