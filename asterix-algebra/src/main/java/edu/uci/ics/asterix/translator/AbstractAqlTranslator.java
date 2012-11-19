package edu.uci.ics.asterix.translator;

import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap.ARTIFACT_KIND;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public abstract class AbstractAqlTranslator {

    protected static final Map<String, BuiltinType> builtinTypeMap = AsterixBuiltinTypeMap.getBuiltinTypes();

    public void validateOperation(Dataverse dataverse, Statement stmt) throws AsterixException {
        String dataverseName = dataverse != null ? dataverse.getDataverseName() : null;
        if (dataverseName != null && dataverseName.equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {

            boolean invalidOperation = false;
            String message = null;
            switch (stmt.getKind()) {
                case INSERT:
                case UPDATE:
                case DELETE:
                    invalidOperation = true;
                    message = " Operation  " + stmt.getKind() + " not permitted in system dataverse " + "'"
                            + MetadataConstants.METADATA_DATAVERSE_NAME + "'";
                    break;
                case FUNCTION_DROP:
                    FunctionSignature signature = ((FunctionDropStatement) stmt).getFunctionSignature();
                    FunctionIdentifier fId = new FunctionIdentifier(signature.getNamespace(), signature.getName(),
                            signature.getArity());
                    if (dataverseName.equals(MetadataConstants.METADATA_DATAVERSE_NAME)
                            && AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.FUNCTION, fId)) {
                        invalidOperation = true;
                        message = "Cannot drop function " + signature + " (protected by system)";
                    }
                    break;
                case NODEGROUP_DROP:
                    NodeGroupDropStatement nodeGroupDropStmt = (NodeGroupDropStatement) stmt;
                    String nodegroupName = nodeGroupDropStmt.getNodeGroupName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.NODEGROUP, nodegroupName)) {
                        message = "Cannot drop nodegroup " + nodegroupName + " (protected by system)";
                        invalidOperation = true;
                    }
                    break;
                case DATAVERSE_DROP:
                    DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                    String dvName = dvDropStmt.getDataverseName().getValue();
                    if (dvName.equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {
                        message = "Cannot drop dataverse " + dvName + " (protected by system)";
                        invalidOperation = true;
                    }
                    break;
                case DATASET_DROP:
                    DropStatement dropStmt = (DropStatement) stmt;
                    String datasetName = dropStmt.getDatasetName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.DATASET, datasetName)) {
                        invalidOperation = true;
                        message = "Cannot drop dataset " + datasetName + " (protected by system)";
                    }
                    break;
            }
            if (invalidOperation) {
                throw new AsterixException(message);
            }
        }
    }
}