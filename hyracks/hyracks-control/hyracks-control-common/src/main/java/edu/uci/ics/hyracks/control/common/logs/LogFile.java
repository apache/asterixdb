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
package edu.uci.ics.hyracks.control.common.logs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.json.JSONObject;

public class LogFile {
    private final File root;

    private PrintWriter out;

    public LogFile(File root) {
        this.root = root;
    }

    public void open() throws Exception {
        root.mkdirs();
        out = new PrintWriter(new FileOutputStream(new File(root, String.valueOf(System.currentTimeMillis()) + ".log"),
                true));
    }

    public void log(JSONObject object) throws Exception {
        out.println(object.toString(1));
        out.flush();
    }

    public void close() {
        out.flush();
        out.close();
    }
}