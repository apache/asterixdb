/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.declared;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class AqlCompiledIndexDecl {

    public enum IndexKind {
        BTREE,
        RTREE
    }

    private String indexName;
    private IndexKind kind;
    private List<String> fieldExprs = new ArrayList<String>();

    public AqlCompiledIndexDecl(String indexName, IndexKind kind, List<String> fieldExprs) {
        this.indexName = indexName;
        this.kind = kind;
        this.fieldExprs = fieldExprs;
    }

    @Override
    public String toString() {
        return "INDEX " + indexName + " (" + kind + ") " + fieldExprs;
    }

    public IndexKind getKind() {
        return kind;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<String> getFieldExprs() {
        return fieldExprs;
    }

    public static IAType keyFieldType(String expr, ARecordType recType) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        for (int i = 0; i < n; i++) {
            if (names[i].equals(expr)) {
                return recType.getFieldTypes()[i];
            }
        }
        throw new AlgebricksException("Could not find field " + expr + " in the schema.");
    }

    public static Pair<IAType, Boolean> getNonNullableKeyFieldType(String expr, ARecordType recType) throws AlgebricksException {
        IAType keyType = AqlCompiledIndexDecl.keyFieldType(expr, recType);
        boolean nullable = false;
        if (keyType.getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) keyType;
            if (unionType.isNullableType()) {
                // The non-null type is always at index 1.
                keyType = unionType.getUnionList().get(1);
                nullable = true;
            }
        }
        return new Pair<IAType, Boolean>(keyType, nullable);
    }
}
