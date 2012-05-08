package edu.uci.ics.asterix.tools.translator;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ADGenDmlTranslator extends AbstractAqlTranslator {

    private final MetadataTransactionContext mdTxnCtx;
    private List<Statement> aqlStatements;
    private AqlCompiledMetadataDeclarations compiledDeclarations;

    public ADGenDmlTranslator(MetadataTransactionContext mdTxnCtx, List<Statement> aqlStatements) {
        this.mdTxnCtx = mdTxnCtx;
        this.aqlStatements = aqlStatements;
    }

    public void translate() throws AlgebricksException, MetadataException {
        compiledDeclarations = compileMetadata(mdTxnCtx, aqlStatements, false);
    }

    public AqlCompiledMetadataDeclarations getCompiledDeclarations() {
        return compiledDeclarations;
    }
}
