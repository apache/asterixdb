/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AsterixTestHelper {
    private static final Logger LOGGER = LogManager.getLogger();

    private AsterixTestHelper() {
    }

    public static String extToResExt(String fname, String resultExt) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + resultExt;
    }

    public static ArrayList<String> readTestListFile(String fileName, String basePath) {
        return readTestListFile(new File(basePath, fileName));
    }

    public static ArrayList<String> readTestListFile(File file) {
        ArrayList<String> list = new ArrayList<>();
        BufferedReader result;
        try {
            result = new BufferedReader(new FileReader(file));
            while (true) {
                String line = result.readLine();
                if (line == null) {
                    break;
                }
                line = line.replaceAll("#.*", "");
                if (line.trim().length() != 0) {
                    list.add(line);
                }
            }
            result.close();
        } catch (IOException e) {
            System.err.println("ignoring " + e.getMessage());
        }
        return list;
    }

    public static void deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                deleteRec(f);
            }
        }
        path.delete();
    }

    public static void deepSelectiveCopy(File srcDir, File destDir, FileFilter filter) throws IOException {
        if (!srcDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + srcDir);
        }
        if (destDir.exists() && !destDir.isDirectory()) {
            throw new IllegalArgumentException("Exists and not a directory: " + destDir);
        }
        for (File child : srcDir.listFiles()) {
            File destChild = new File(destDir, child.getName());
            if (child.isDirectory()) {
                deepSelectiveCopy(child, destChild, filter);
            } else if (filter.accept(child)) {
                FileUtil.safeCopyFile(child, destChild);
                return;
            }
        }
    }

}
