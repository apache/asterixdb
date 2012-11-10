package edu.uci.ics.asterix.test.aql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

public class AQLTestCase extends TestCase {

    private File queryFile;

    AQLTestCase(File queryFile) {
        super("testAQL");
        this.queryFile = queryFile;
    }

    @Test
    public void testAQL() throws UnsupportedEncodingException, FileNotFoundException, ParseException, AsterixException,
            AlgebricksException {
        Reader fis = new BufferedReader(new InputStreamReader(new FileInputStream(queryFile), "UTF-8"));
        AQLParser parser = new AQLParser(fis);
        List<Statement> statements;
        GlobalConfig.ASTERIX_LOGGER.info(queryFile.toString());
        try {
            statements = parser.Statement();
        } catch (ParseException e) {
            GlobalConfig.ASTERIX_LOGGER.warning("Failed while testing file " + fis);
            StringWriter sw = new StringWriter();
            PrintWriter writer = new PrintWriter(sw);
            e.printStackTrace(writer);
            GlobalConfig.ASTERIX_LOGGER.warning(sw.toString());
            throw new ParseException("Parsing " + queryFile.toString());
        }

    }
}
