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
package edu.uci.ics.hyracks.hadoop.compat.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

public class Utilities {

    public static Properties getProperties(String filePath, char delimiter) {
        Properties properties = new Properties();
        try {
            FileInputStream fins = new FileInputStream(new File(filePath));
            DataInputStream dins = new DataInputStream(fins);
            BufferedReader br = new BufferedReader(new InputStreamReader(dins));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                int split = strLine.indexOf(delimiter);
                if (split >= 0) {
                    properties.put((strLine.substring(0, split)).trim(), strLine.substring(split + 1, strLine.length())
                            .trim());
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return properties;
    }

    public static File getHyracksArchive(String applicationName, Set<String> libJars) {
       String target = applicationName + ".zip";
        // Create a buffer for reading the files
        byte[] buf = new byte[1024];
        Set<String> fileNames = new HashSet<String>();
        try {
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(target));
            for (String libJar : libJars) {
                String fileName = libJar.substring(libJar.lastIndexOf("/") + 1);
                if(fileNames.contains(fileName)){
                    continue;
                }
                FileInputStream in = new FileInputStream(libJar);
                out.putNextEntry(new ZipEntry(fileName));
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                out.closeEntry();
                in.close();
                fileNames.add(fileName);
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        File har = new File(target);
        har.deleteOnExit();
        return har;
    }
    
    public static Reporter createReporter() {
        Reporter reporter = new Reporter() {

            @Override
            public void progress() {

            }

            @Override
            public void setStatus(String arg0) {

            }

            @Override
            public void incrCounter(String arg0, String arg1, long arg2) {

            }

            @Override
            public void incrCounter(Enum<?> arg0, long arg1) {

            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public Counter getCounter(String arg0, String arg1) {
                return null;
            }

            @Override
            public Counter getCounter(Enum<?> arg0) {
                return null;
            }
        };
        return reporter;
    }
}
