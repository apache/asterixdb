package edu.uci.ics.asterix.translator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public abstract class AbstractAqlTranslator {

    public AqlCompiledMetadataDeclarations compileMetadata(MetadataTransactionContext mdTxnCtx,
            List<Statement> statements, boolean online) throws AlgebricksException, MetadataException {
        List<TypeDecl> typeDeclarations = new ArrayList<TypeDecl>();
        Map<String, String> config = new HashMap<String, String>();

        FileSplit outputFile = null;
        IAWriterFactory writerFactory = null;
        String dataverseName = MetadataConstants.METADATA_DATAVERSE_NAME;
        for (Statement stmt : statements) {
            switch (stmt.getKind()) {
                case TYPE_DECL: {
                    typeDeclarations.add((TypeDecl) stmt);
                    break;
                }
                case DATAVERSE_DECL: {
                    DataverseDecl dstmt = (DataverseDecl) stmt;
                    dataverseName = dstmt.getDataverseName().toString();
                    break;
                }
                case WRITE: {
                    if (outputFile != null) {
                        throw new AlgebricksException("Multiple 'write' statements.");
                    }
                    WriteStatement ws = (WriteStatement) stmt;
                    File f = new File(ws.getFileName());
                    outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
                    if (ws.getWriterClassName() != null) {
                        try {
                            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
                        } catch (Exception e) {
                            throw new AlgebricksException(e);
                        }
                    }
                    break;
                }
                case SET: {
                    SetStatement ss = (SetStatement) stmt;
                    String pname = ss.getPropName();
                    String pvalue = ss.getPropValue();
                    config.put(pname, pvalue);
                    break;
                }
            }
        }
        if (writerFactory == null) {
            writerFactory = PrinterBasedWriterFactory.INSTANCE;
        }

        MetadataDeclTranslator metadataTranslator = new MetadataDeclTranslator(mdTxnCtx, dataverseName, outputFile,
                writerFactory, config, typeDeclarations);
        return metadataTranslator.computeMetadataDeclarations(online);
    }

    public void validateOperation(AqlCompiledMetadataDeclarations compiledDeclarations, Statement stmt)
            throws AlgebricksException {
        if (compiledDeclarations.getDataverseName() != null
                && compiledDeclarations.getDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {
            if (stmt.getKind() == Statement.Kind.INSERT || stmt.getKind() == Statement.Kind.UPDATE
                    || stmt.getKind() == Statement.Kind.DELETE) {
                throw new AlgebricksException(" Operation  " + stmt.getKind() + " not permitted in system dataverse-"
                        + MetadataConstants.METADATA_DATAVERSE_NAME);
            }
        }
    }
}