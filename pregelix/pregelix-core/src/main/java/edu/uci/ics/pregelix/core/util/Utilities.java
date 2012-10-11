package edu.uci.ics.pregelix.core.util;

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
            br.close();
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
                if (fileNames.contains(fileName)) {
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
}
