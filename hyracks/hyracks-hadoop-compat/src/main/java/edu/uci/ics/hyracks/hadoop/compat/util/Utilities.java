/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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

    public static File getHyracksArchive(String applicationName, String[] libJars) {
        String target = applicationName + ".zip";
        // Create a buffer for reading the files
        byte[] buf = new byte[1024];
        try {
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(target));
            for (int i = 0; i < libJars.length; i++) {
                String fileName = libJars[i].substring(libJars[i].lastIndexOf("/") + 1);
                FileInputStream in = new FileInputStream(libJars[i]);
                out.putNextEntry(new ZipEntry(fileName));
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                out.closeEntry();
                in.close();
            }
            out.close();
        } catch (IOException e) {
        }
        File har = new File(target);
        har.deleteOnExit();
        return har;
    }

}
