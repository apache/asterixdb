package edu.uci.ics.asterix.tools.translator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeSignature;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.asterix.translator.TypeTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ADGenDmlTranslator extends AbstractAqlTranslator {

    private final MetadataTransactionContext mdTxnCtx;
    private final List<Statement> aqlStatements;
    private Map<TypeSignature, IAType> types;
    private Map<TypeSignature, TypeDataGen> typeDataGenMap;

    public ADGenDmlTranslator(MetadataTransactionContext mdTxnCtx, List<Statement> aqlStatements) {
        this.mdTxnCtx = mdTxnCtx;
        this.aqlStatements = aqlStatements;
    }

    public void translate() throws AsterixException, MetadataException, AlgebricksException {
        String defaultDataverse = getDefaultDataverse();
        types = new HashMap<TypeSignature, IAType>();
        typeDataGenMap = new HashMap<TypeSignature, TypeDataGen>();

        for (Statement stmt : aqlStatements) {
            if (stmt.getKind().equals(Statement.Kind.TYPE_DECL)) {
                TypeDecl td = (TypeDecl) stmt;
                String typeDataverse = td.getDataverseName() == null ? defaultDataverse : td.getDataverseName()
                        .getValue();

                Map<TypeSignature, IAType> typeInStmt = TypeTranslator.computeTypes(mdTxnCtx, td, typeDataverse, types);
                types.putAll(typeInStmt);

                TypeSignature signature = new TypeSignature(typeDataverse, td.getIdent().getValue());
                TypeDataGen tdg = td.getDatagenAnnotation();
                if (tdg != null) {
                    typeDataGenMap.put(signature, tdg);
                }
            }
        }
    }

    private String getDefaultDataverse() {
        for (Statement stmt : aqlStatements) {
            if (stmt.getKind().equals(Statement.Kind.DATAVERSE_DECL)) {
                return ((DataverseDecl) stmt).getDataverseName().getValue();
            }
        }
        return null;
    }

    public Map<TypeSignature, IAType> getTypeMap() {
        return types;
    }

    public Map<TypeSignature, TypeDataGen> getTypeDataGenMap() {
        return typeDataGenMap;
    }

}
