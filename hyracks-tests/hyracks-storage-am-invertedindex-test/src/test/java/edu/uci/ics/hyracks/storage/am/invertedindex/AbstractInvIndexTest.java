package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.AfterClass;

public abstract class AbstractInvIndexTest {

    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected final static String baseFileName = tmpDir + sep + simpleDateFormat.format(new Date());
    protected final static String btreeFileName =  baseFileName + "btree";
    protected final static String invListsFileName = baseFileName + "invlists";

    protected void print(String str) {
        System.out.print(str);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File btreeFile = new File(btreeFileName);
        btreeFile.deleteOnExit();
        File invListsFile = new File(invListsFileName);
        invListsFile.deleteOnExit();
    }
}
