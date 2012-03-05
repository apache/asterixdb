package edu.uci.ics.asterix.api.java;

import java.io.PrintWriter;
import java.io.Reader;
import java.rmi.RemoteException;

import org.json.JSONException;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class AsterixJavaClient {

    private Reader queryText;
    private PrintWriter writer;

    private Job[] dmlJobs;
    private JobSpecification queryJobSpec;

    public AsterixJavaClient(Reader queryText, PrintWriter writer) {
        this.queryText = queryText;
        this.writer = writer;
    }

    public AsterixJavaClient(Reader queryText) {
        this.queryText = queryText;
        this.writer = new PrintWriter(System.out, true);
    }

    public void compile() throws Exception {
        compile(true, false, false, false, false, false, false);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob)
            throws AsterixException, AlgebricksException, JSONException, RemoteException, ACIDException {
        queryJobSpec = null;
        dmlJobs = null;

        if (queryText == null) {
            return;
        }
        AQLParser parser = new AQLParser(queryText);
        Query q;
        try {
            q = (Query) parser.Statement();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }
        MetadataManager.INSTANCE.init();

        SessionConfig pc = new SessionConfig(AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT, optimize, false,
                printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printPhysicalOpsOnly, printJob);
        pc.setGenerateJobSpec(generateBinaryRuntime);

        String dataverseName = null;
        if (q != null) {
            dataverseName =  APIFramework.compileDdlStatements(q, writer, pc, DisplayFormat.TEXT);
            dmlJobs = APIFramework.compileDmlStatements(dataverseName, q, writer, pc, DisplayFormat.TEXT);
        }

        if (q.isDummyQuery()) {
            return;
        }

        Pair<AqlCompiledMetadataDeclarations, JobSpecification> metadataAndSpec = APIFramework.compileQuery(dataverseName, q,
                parser.getVarCounter(), null, null, pc, writer, DisplayFormat.TEXT, null);
        if (metadataAndSpec != null) {
            queryJobSpec = metadataAndSpec.second;
        }
        writer.flush();
    }

    public void execute(int port) throws Exception {
        if (dmlJobs != null) {
            APIFramework.executeJobArray(dmlJobs, port, writer, DisplayFormat.TEXT);
        }
        if (queryJobSpec != null) {
            APIFramework.executeJobArray(new JobSpecification[] { queryJobSpec }, port, writer, DisplayFormat.TEXT);
        }
    }

}