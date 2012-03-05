package edu.uci.ics.asterix.context;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class AsterixFileMapManager implements IFileMapManager {

    private static final long serialVersionUID = 1L;
    private Map<Integer, String> id2nameMap = new HashMap<Integer, String>();
    private Map<String, Integer> name2IdMap = new HashMap<String, Integer>();
    private int idCounter = 0;

    @Override
    public FileReference lookupFileName(int fileId) throws HyracksDataException {
        String fName = id2nameMap.get(fileId);
        if (fName == null) {
            throw new HyracksDataException("No mapping found for id: " + fileId);
        }
        return new FileReference(new File(fName));
    }

    @Override
    public int lookupFileId(FileReference fileRef) throws HyracksDataException {
        String fileName = fileRef.getFile().getAbsolutePath();
        Integer fileId = name2IdMap.get(fileName);
        if (fileId == null) {
            throw new HyracksDataException("No mapping found for name: " + fileName);
        }
        return fileId;
    }

    @Override
    public boolean isMapped(FileReference fileRef) {
        String fileName = fileRef.getFile().getAbsolutePath();
        return name2IdMap.containsKey(fileName);
    }

    @Override
    public boolean isMapped(int fileId) {
        return id2nameMap.containsKey(fileId);
    }

    @Override
    public void unregisterFile(int fileId) throws HyracksDataException {
        String fileName = id2nameMap.remove(fileId);
        name2IdMap.remove(fileName);
    }

    @Override
    public void registerFile(FileReference fileRef) throws HyracksDataException {
        Integer fileId = idCounter++;
        String fileName = fileRef.getFile().getAbsolutePath();
        id2nameMap.put(fileId, fileName);
        name2IdMap.put(fileName, fileId);
    }
}
