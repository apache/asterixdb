package edu.uci.ics.asterix.hyracks.bootstrap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import edu.uci.ics.hyracks.api.application.IStateDumpHandler;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponentManager;

public class AsterixStateDumpHandler implements IStateDumpHandler {
    private final String nodeId;
    private final Path dumpPath;
    private final ILifeCycleComponentManager lccm;

    public AsterixStateDumpHandler(String nodeId, String dumpPath, ILifeCycleComponentManager lccm) {
        this.nodeId = nodeId;
        this.dumpPath = Paths.get(dumpPath);
        this.lccm = lccm;
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        dumpPath.toFile().mkdirs();
        File df = dumpPath.resolve(nodeId + "-" + System.currentTimeMillis() + ".dump").toFile();
        try (FileOutputStream fos = new FileOutputStream(df)) {
            lccm.dumpState(fos);
        }
        os.write(df.getAbsolutePath().getBytes("UTF-8"));
    }

}
