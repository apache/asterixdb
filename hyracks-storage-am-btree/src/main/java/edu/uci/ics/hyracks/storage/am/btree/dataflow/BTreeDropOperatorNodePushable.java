package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.File;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BTreeDropOperatorNodePushable extends AbstractOperatorNodePushable {

    private String btreeFileName;
    private IBTreeRegistryProvider btreeRegistryProvider;
    private IBufferCacheProvider bufferCacheProvider;
    private IFileMappingProviderProvider fileMappingProviderProvider;

    public BTreeDropOperatorNodePushable(IBufferCacheProvider bufferCacheProvider,
            IBTreeRegistryProvider btreeRegistryProvider, String btreeFileName,
            IFileMappingProviderProvider fileMappingProviderProvider) {
        this.btreeFileName = btreeFileName;
        this.fileMappingProviderProvider = fileMappingProviderProvider;
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

        File f = new File(fileName);
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
