package edu.uci.ics.asterix.test.aql;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.json.JSONArray;
import org.json.JSONException;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class TestsUtils {

    private static final String EXTENSION_AQL_RESULT = "adm";

    /**
     * Probably does not work well with symlinks.
     */
    public static boolean deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                if (!deleteRec(f)) {
                    return false;
                }
            }
        }
        return path.delete();
    }

    public static String getNextResult(JSONArray jArray) throws JSONException {
        String result = null;
        for (int i = 0; i < jArray.length(); i++) {
            JSONArray resultArray = jArray.getJSONArray(i);
            for (int j = 0; j < resultArray.length(); j++) {
                return resultArray.getString(j);
            }
        }
        return result;
    }

    public static void runScriptAndCompareWithResult(IHyracksClientConnection hcc, File scriptFile, PrintWriter print,
            File expectedFile, JSONArray jArray) throws Exception {
        BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                "UTF-8"));

        String lineExpected, lineActual;
        int num = 0;
        int chunkCounter = 0;
        int recordCounter = 0;
        try {

            while ((lineExpected = readerExpected.readLine()) != null) {
                if (jArray.length() <= 0) {
                    throw new Exception("No results returned for query.");
                }
                JSONArray resultArray = jArray.getJSONArray(chunkCounter);

                if ((lineActual = resultArray.getString(recordCounter)) == null) {
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n<" + lineExpected
                            + "\n>");

                }
                if (!equalStrings(lineExpected.split("Timestamp")[0], lineActual.split("Timestamp")[0])) {
                    fail("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + lineActual);
                }

                recordCounter++;
                if (recordCounter >= resultArray.length()) {
                    chunkCounter++;
                    recordCounter = 0;
                    if (chunkCounter >= jArray.length()) {
                        break;
                    }
                }
            }

            while ((lineExpected = readerExpected.readLine()) != null) {
                // TODO(khurram): Print out the remaining expected file contents
            }
        } finally {
            readerExpected.close();
        }

    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split(" ");
            String[] fields2 = row2.split(" ");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    fields1[j] = fields1[j].split(",")[0];
                    fields2[j] = fields2[j].split(",")[0];
                    Double double1 = Double.parseDouble(fields1[j]);
                    Double double2 = Double.parseDouble(fields2[j]);
                    float float1 = (float) double1.doubleValue();
                    float float2 = (float) double2.doubleValue();

                    if (Math.abs(float1 - float2) == 0)
                        continue;
                    else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static String aqlExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + EXTENSION_AQL_RESULT;
    }

}
