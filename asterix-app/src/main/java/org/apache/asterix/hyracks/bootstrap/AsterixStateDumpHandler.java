/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
