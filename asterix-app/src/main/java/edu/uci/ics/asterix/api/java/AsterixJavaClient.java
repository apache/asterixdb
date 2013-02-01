package edu.uci.ics.asterix.api.java;

import java.io.PrintWriter;
import java.io.Reader;
import java.util.List;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class AsterixJavaClient {
    private IHyracksClientConnection hcc;
    private Reader queryText;
    private PrintWriter writer;

    private Job[] dmlJobs;
    private JobSpecification queryJobSpec;

    public AsterixJavaClient(IHyracksClientConnection hcc, Reader queryText, PrintWriter writer) {
        this.hcc = hcc;
        this.queryText = queryText;
        this.writer = writer;
    }

    public AsterixJavaClient(IHyracksClientConnection hcc, Reader queryText) {
        this(hcc, queryText, new PrintWriter(System.out, true));
    }

    public void compile() throws Exception {
        compile(true, false, false, false, false, false, false);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob)
            throws Exception {
        queryJobSpec = null;
        dmlJobs = null;

        if (queryText == null) {
            return;
        }
        int ch;
        StringBuilder builder = new StringBuilder();
        while ((ch = queryText.read()) != -1) {
            builder.append((char)ch);
        }
        AQLParser parser = new AQLParser(builder.toString());
        List<Statement> aqlStatements;
        try {
            aqlStatements = parser.Statement();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }
        MetadataManager.INSTANCE.init();

        SessionConfig pc = new SessionConfig(AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT, optimize,
                false, printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printPhysicalOpsOnly,
                generateBinaryRuntime, printJob);

        AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, writer, pc, DisplayFormat.TEXT);
        aqlTranslator.compileAndExecute(hcc);
        writer.flush();
    }

    public void execute() throws Exception {
        if (dmlJobs != null) {
            APIFramework.executeJobArray(hcc, dmlJobs, writer, DisplayFormat.TEXT);
        }
        if (queryJobSpec != null) {
            APIFramework.executeJobArray(hcc, new JobSpecification[] { queryJobSpec }, writer, DisplayFormat.TEXT);
        }
    }

}