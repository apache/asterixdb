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
package edu.uci.ics.pregelix.api.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * @author yingyib
 */
public class JobStateUtils {

    public static final String TMP_DIR = "/tmp/";

    public static void writeForceTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = TMP_DIR + jobId + "fterm";
            Path path = new Path(pathStr);
            if (!dfs.exists(path)) {
                FSDataOutputStream output = dfs.create(path, true);
                output.writeBoolean(true);
                output.flush();
                output.close();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static boolean readForceTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = TMP_DIR + jobId + "fterm";
            Path path = new Path(pathStr);
            if (dfs.exists(path)) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
