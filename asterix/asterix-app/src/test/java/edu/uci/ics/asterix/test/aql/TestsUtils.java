package edu.uci.ics.asterix.test.aql;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import edu.uci.ics.asterix.api.aqlj.client.AQLJClientDriver;
import edu.uci.ics.asterix.api.aqlj.client.IADMCursor;
import edu.uci.ics.asterix.api.aqlj.client.IAQLJConnection;
import edu.uci.ics.asterix.api.aqlj.client.IAQLJResult;
import edu.uci.ics.asterix.api.aqlj.common.AQLJException;
import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;

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

    public static void runScriptAndCompareWithResult(File scriptFile, PrintWriter print, File expectedFile,
            File actualFile) throws Exception {
        Reader query = new BufferedReader(new FileReader(scriptFile));
        AsterixJavaClient asterix = new AsterixJavaClient(query, print);
        try {
            asterix.compile(true, false, false, false, false, true, false);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + scriptFile + ": " + e.getMessage(), e);
        }
        asterix.execute(AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT);
        query.close();
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
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

                if (!lineExpected.split("Timestamp")[0].equals(lineActual.split("Timestamp")[0])) {
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

    public static void runScriptAndCompareWithResultViaClientAPI(File scriptFile, PrintWriter print, File expectedFile,
            File actualFile, int apiPort) throws Exception {
        FileOutputStream fos = new FileOutputStream(actualFile);
        String query = queryFromFile(scriptFile);
        IAQLJConnection conn = null;
        IAQLJResult res = null;
        try {
            conn = AQLJClientDriver.getConnection("localhost", apiPort, "Metadata");
            res = conn.execute(query);

            while (res.next()) {
                leafPrint(conn, res, fos);
            }
        } catch (AQLJException e) {
            e.printStackTrace();
        } finally {
            // be sure that we close the connection and the result cursor
            if (res != null) {
                res.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        fos.close();

        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
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
                if (!equalStrings(lineExpected, lineActual)) {
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> " + lineActual);
                }
                ++num;
            }
            lineActual = readerActual.readLine();
            // Assert.assertEquals(null, lineActual);
            if (lineActual != null) {
                throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< \n> " + lineActual);
            }
            actualFile.delete();
        } finally {
            readerExpected.close();
            readerActual.close();
        }

    }

    public static void leafPrint(IAQLJConnection conn, IADMCursor c, FileOutputStream fos) throws AQLJException,
            UnsupportedEncodingException, IOException {
        IAType t;
        IAObject o;
        String fieldNames[];
        IADMCursor cur;

        o = c.get();
        if (o == null) {
            return;
        }

        t = o.getType();
        if (t instanceof AbstractCollectionType) {
            fos.write("AbstractCollectionType: \n".getBytes("UTF-8"));
            cur = conn.createADMCursor();
            c.position(cur);
            while (cur.next()) {

                leafPrint(conn, cur, fos);
            }
        } else if (t instanceof ARecordType) {
            fos.write("ARecordType: \n".getBytes("UTF-8"));
            fieldNames = ((ARecordType) t).getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                cur = conn.createADMCursor();
                c.position(cur, fieldNames[i]);
                fos.write(("field: " + fieldNames[i] + "\n").getBytes("UTF-8"));
                leafPrint(conn, cur, fos);
            }
        } else {
            fos.write((o.toString() + "\n").getBytes("UTF-8"));
        }
    }

    private static String queryFromFile(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        try {
            FileChannel fc = fis.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            return Charset.forName("UTF-8").decode(bb).toString();
        } finally {
            fis.close();
        }

    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        if (rowsOne.length != rowsTwo.length)
            return false;

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
