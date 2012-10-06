package edu.uci.ics.hyracks.storage.common.smi;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class TransientFileMapManager implements IFileMapManager {
    private static final long serialVersionUID = 1L;

    private Map<Integer, FileReference> id2nameMap = new HashMap<Integer, FileReference>();
    private Map<FileReference, Integer> name2IdMap = new HashMap<FileReference, Integer>();
    private int idCounter = 0;

    @Override
    public FileReference lookupFileName(int fileId) throws HyracksDataException {
        FileReference fRef = id2nameMap.get(fileId);
        if (fRef == null) {
            throw new HyracksDataException("No mapping found for id: " + fileId);
        }
        return fRef;
    }

    @Override
    public int lookupFileId(FileReference fileRef) throws HyracksDataException {
        Integer fileId = name2IdMap.get(fileRef);
        if (fileId == null) {
            throw new HyracksDataException("No mapping found for name: " + fileRef);
        }
        return fileId;
    }

    @Override
    public boolean isMapped(FileReference fileRef) {
        return name2IdMap.containsKey(fileRef);
    }

    @Override
    public boolean isMapped(int fileId) {
        return id2nameMap.containsKey(fileId);
    }

    @Override
    public void unregisterFile(int fileId) throws HyracksDataException {
        FileReference fileRef = id2nameMap.remove(fileId);
        name2IdMap.remove(fileRef);
    }

    @Override
    public void registerFile(FileReference fileRef) throws HyracksDataException {
        Integer fileId = idCounter++;
        id2nameMap.put(fileId, fileRef);
        name2IdMap.put(fileRef, fileId);
    }
}