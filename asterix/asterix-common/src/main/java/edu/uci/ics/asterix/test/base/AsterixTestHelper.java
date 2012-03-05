package edu.uci.ics.asterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class AsterixTestHelper {

    public static String extToResExt(String fname, String resultExt) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + resultExt;
    }

    public static ArrayList<String> readFile(String fileName, String basePath) {
        ArrayList<String> list = new ArrayList<String>();
        BufferedReader result;
        try {
            result = new BufferedReader(new FileReader(basePath + fileName));
            while (true) {
                String line = result.readLine();
                if (line == null) {
                    break;
                }
                if (line.length() == 0) {
                    continue;
                } else {
                    list.add(line);
                }
            }
            result.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
        return list;
    }

    public static void readFileToString(File file, StringBuilder buf) throws Exception {
        BufferedReader result = new BufferedReader(new FileReader(file));
        while (true) {
            String s = result.readLine();
            if (s == null) {
                break;
            } else {
                buf.append(s);
                buf.append('\n');
            }
        }
        result.close();
    }

    public static void deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                deleteRec(f);
            }
        }
        path.delete();
    }
}
