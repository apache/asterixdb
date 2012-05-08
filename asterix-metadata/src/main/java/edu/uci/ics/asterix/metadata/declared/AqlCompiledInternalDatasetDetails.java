/*
 * Copyright 2009-2011 by The Regents of the University of California
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
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;

public class AqlCompiledInternalDatasetDetails implements IAqlCompiledDatasetDetails {
    private final List<String> partitioningExprs;
    private final List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitionFuns;
    private final String nodegroupName;
    private final List<AqlCompiledIndexDecl> secondaryIndexes;
    private final AqlCompiledIndexDecl primaryIndex;
    private HashMap<String, List<AqlCompiledIndexDecl>> secondaryIndexInvertedList;

    public AqlCompiledInternalDatasetDetails(List<String> partitioningExprs,
            List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitionFuns, String nodegroupName,
            AqlCompiledIndexDecl primaryIndex, List<AqlCompiledIndexDecl> secondaryIndexes) {
        this.partitioningExprs = partitioningExprs;
        this.partitionFuns = partitionFuns;
        this.nodegroupName = nodegroupName;
        this.primaryIndex = primaryIndex;
        this.secondaryIndexes = secondaryIndexes;
        invertSecondaryIndexExprs();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("dataset partitioned-by " + partitionFuns + " on " + nodegroupName);
        if (secondaryIndexes != null && !secondaryIndexes.isEmpty()) {
            sb.append(System.getProperty("line.separator") + " with indexes: " + secondaryIndexes);
        }
        return sb.toString();
    }

    public List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> getPartitioningFunctions() {
        return partitionFuns;
    }

    public List<String> getPartitioningExprs() {
        return partitioningExprs;
    }

    public int getPositionOfPartitioningKeyField(String fieldName) {
        int pos = 0;
        for (String pe : partitioningExprs) {
            if (pe.equals(fieldName)) {
                return pos;
            }
            ++pos;
        }
        return -1;
    }

    public String getNodegroupName() {
        return nodegroupName;
    }

    public List<AqlCompiledIndexDecl> getSecondaryIndexes() {
        return secondaryIndexes;
    }

    public AqlCompiledIndexDecl getPrimaryIndex() {
        return primaryIndex;
    }

    public List<AqlCompiledIndexDecl> findSecondaryIndexesByOneOfTheKeys(String fieldExpr) {
        return secondaryIndexInvertedList.get(fieldExpr);
    }

    public AqlCompiledIndexDecl findSecondaryIndexByExactKeyList(List<String> fieldExprs) {
        if (secondaryIndexes == null) {
            return null;
        }
        for (AqlCompiledIndexDecl acid : secondaryIndexes) {
            if (acid.getFieldExprs().equals(fieldExprs)) {
                return acid;
            }
        }
        return null;
    }

    public AqlCompiledIndexDecl findSecondaryIndexByName(String idxName) {
        if (secondaryIndexes == null) {
            return null;
        }
        for (AqlCompiledIndexDecl acid : secondaryIndexes) {
            if (acid.getIndexName().equals(idxName)) {
                return acid;
            }
        }
        return null;
    }

    private void invertSecondaryIndexExprs() {
        secondaryIndexInvertedList = new HashMap<String, List<AqlCompiledIndexDecl>>();
        if (secondaryIndexes == null) {
            return;
        }
        for (AqlCompiledIndexDecl idx : secondaryIndexes) {
            for (String s : idx.getFieldExprs()) {
                List<AqlCompiledIndexDecl> idxList = secondaryIndexInvertedList.get(s);
                if (idxList == null) {
                    idxList = new ArrayList<AqlCompiledIndexDecl>();
                    secondaryIndexInvertedList.put(s, idxList);
                }
                idxList.add(idx);
            }
        }
    }

    @Override
    public DatasetType getDatasetType() {
        return DatasetType.INTERNAL;
    }
}
