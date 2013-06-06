/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.translator;

import java.util.Map;
import java.util.Map.Entry;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.dataset.hints.DatasetHints;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 * Base class for AQL translators. Contains the common validation logic for AQL
 * statements.
 */
public abstract class AbstractAqlTranslator {

    protected static final Map<String, BuiltinType> builtinTypeMap = AsterixBuiltinTypeMap.getBuiltinTypes();

    public void validateOperation(Dataverse defaultDataverse, Statement stmt) throws AsterixException {

        if (AsterixClusterProperties.INSTANCE.getState().equals(AsterixClusterProperties.State.UNUSABLE)) {
            throw new AsterixException(" Asterix Cluster is in " + AsterixClusterProperties.State.UNUSABLE + " state."
                    + "\n One or more Node Controllers have left.\n");
        }

        boolean invalidOperation = false;
        String message = null;
        String dataverse = defaultDataverse != null ? defaultDataverse.getDataverseName() : null;
        switch (stmt.getKind()) {
            case INSERT:
                InsertStatement insertStmt = (InsertStatement) stmt;
                if (insertStmt.getDataverseName() != null) {
                    dataverse = insertStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Insert operation is not permitted in dataverse "
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;

            case DELETE:
                DeleteStatement deleteStmt = (DeleteStatement) stmt;
                if (deleteStmt.getDataverseName() != null) {
                    dataverse = deleteStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Delete operation is not permitted in dataverse "
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;

            case NODEGROUP_DROP:
                String nodegroupName = ((NodeGroupDropStatement) stmt).getNodeGroupName().getValue();
                invalidOperation = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME.equals(nodegroupName);
                if (invalidOperation) {
                    message = "Cannot drop nodegroup:" + nodegroupName;
                }
                break;

            case DATAVERSE_DROP:
                DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dvDropStmt.getDataverseName()
                        .getValue());
                if (invalidOperation) {
                    message = "Cannot drop dataverse:" + dvDropStmt.getDataverseName().getValue();
                }
                break;

            case DATASET_DROP:
                DropStatement dropStmt = (DropStatement) stmt;
                if (dropStmt.getDataverseName() != null) {
                    dataverse = dropStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Cannot drop a dataset belonging to the dataverse:"
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;
            case DATASET_DECL:
                DatasetDecl datasetStmt = (DatasetDecl) stmt;
                Map<String, String> hints = datasetStmt.getHints();
                if (hints != null && !hints.isEmpty()) {
                    Pair<Boolean, String> validationResult = null;
                    StringBuffer errorMsgBuffer = new StringBuffer();
                    for (Entry<String, String> hint : hints.entrySet()) {
                        validationResult = DatasetHints.validate(hint.getKey(), hint.getValue());
                        if (!validationResult.first) {
                            errorMsgBuffer.append("Dataset: " + datasetStmt.getName().getValue()
                                    + " error in processing hint: " + hint.getKey() + " " + validationResult.second);
                            errorMsgBuffer.append(" \n");
                        }
                    }
                    invalidOperation = errorMsgBuffer.length() > 0;
                    if (invalidOperation) {
                        message = errorMsgBuffer.toString();
                    }
                }
                break;

        }

        if (invalidOperation) {
            throw new AsterixException("Invalid operation - " + message);
        }
    }
}
