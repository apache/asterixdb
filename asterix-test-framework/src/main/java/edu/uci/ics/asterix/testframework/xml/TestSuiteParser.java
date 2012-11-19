package edu.uci.ics.asterix.testframework.xml;

import java.io.File;

import javax.xml.bind.JAXBContext;

public class TestSuiteParser {
    public TestSuiteParser() {
    }

    public TestSuite parse(File testSuiteCatalog) throws Exception {
        JAXBContext ctx = JAXBContext.newInstance(TestSuite.class);
        return (TestSuite) ctx.createUnmarshaller().unmarshal(testSuiteCatalog);
    }
}