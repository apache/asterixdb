package edu.uci.ics.asterix.test.aql;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;

import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
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

    public static void runScriptAndCompareWithResult(IHyracksClientConnection hcc, File scriptFile, PrintWriter print,
            File expectedFile, File actualFile) throws Exception {
        Reader query = new BufferedReader(new InputStreamReader(new FileInputStream(scriptFile), "UTF-8"));
        AsterixJavaClient asterix = new AsterixJavaClient(hcc, query, print);
        try {
            asterix.compile(true, false, true, true, false, true, false);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + scriptFile + ": " + e.getMessage(), e);
        } finally {
            query.close();
        }
        asterix.execute();
        BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                "UTF-8"));
        BufferedReader readerActual = new BufferedReader(
                new InputStreamReader(new FileInputStream(actualFile), "UTF-8"));
        String lineExpected, lineActual;
        int num = 1;
        try {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                // Assert.assertEquals(lineExpected, lineActual);
                if (lineActual == null) {
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> ");
                }

                if (!equalStrings(lineExpected.split("Timestamp")[0], lineActual.split("Timestamp")[0])) {
                    fail("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + lineActual);
                }

                /*
                 * if (!equalStrings(lineExpected, lineActual)) { throw new
                 * Exception("Result for " + scriptFile + " changed at line " +
                 * num + ":\n< " + lineExpected + "\n> " + lineActual); }
                 */
                ++num;
            }
            lineActual = readerActual.readLine();
            // Assert.assertEquals(null, lineActual);
            if (lineActual != null) {
                throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< \n> " + lineActual);
            }
            // actualFile.delete();
        } finally {
            readerExpected.close();
            readerActual.close();
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
