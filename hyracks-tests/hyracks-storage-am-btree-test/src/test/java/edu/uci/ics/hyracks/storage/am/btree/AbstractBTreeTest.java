package edu.uci.ics.hyracks.storage.am.btree;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

import org.junit.AfterClass;

public abstract class AbstractBTreeTest {

    protected static final Logger LOGGER = Logger.getLogger(AbstractBTreeTest.class.getName());

    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected final static String fileName = tmpDir + sep + simpleDateFormat.format(new Date());

    protected void print(String str) {
        System.out.print(str);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File f = new File(fileName);
        f.deleteOnExit();
    }
}
