/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.tools.translator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.DataverseDecl;
import org.apache.asterix.aql.expression.TypeDecl;
import org.apache.asterix.common.annotations.TypeDataGen;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.translator.AbstractAqlTranslator;
import org.apache.asterix.translator.TypeTranslator;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

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

                Map<TypeSignature, IAType> typeInStmt = TypeTranslator.computeTypes(mdTxnCtx, td.getTypeDef(), td
                        .getIdent().getValue(), typeDataverse, types);
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
