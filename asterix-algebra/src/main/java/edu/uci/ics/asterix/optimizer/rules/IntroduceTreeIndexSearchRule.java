package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;

public abstract class IntroduceTreeIndexSearchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * just picks the first index for which all the expressions are mentioned
     */
    protected AqlCompiledIndexDecl findUsableIndex(AqlCompiledDatasetDecl ddecl,
            HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIdxExprs) {
        for (AqlCompiledIndexDecl acid : foundIdxExprs.keySet()) {
            List<Pair<String, Integer>> psiList = foundIdxExprs.get(acid);
            boolean allUsed = true;
            for (String keyField : acid.getFieldExprs()) {
                boolean foundKf = false;
                for (Pair<String, Integer> psi : psiList) {
                    if (psi.first.equals(keyField)) {
                        foundKf = true;
                        break;
                    }
                }
                if (!foundKf) {
                    allUsed = false;
                    break;
                }
            }
            if (allUsed) {
                return acid;
            }
        }
        return null;
    }

    protected static ConstantExpression mkStrConstExpr(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }

    protected boolean findIdxExprs(AqlCompiledDatasetDecl ddecl, List<String> primIdxFields,
            AqlCompiledIndexDecl primIdxDecl, HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIdxExprs,
            ArrayList<LogicalVariable> comparedVars, LogicalVariable var, String fieldName) {
        boolean foundVar = false;
        List<AqlCompiledIndexDecl> idxList = DatasetUtils.findSecondaryIndexesByOneOfTheKeys(ddecl, fieldName);
        if (primIdxFields.contains(fieldName)) {
            if (idxList == null) {
                idxList = new ArrayList<AqlCompiledIndexDecl>(1);
            }
            idxList.add(primIdxDecl);
        }
        if (idxList != null) {
            foundVar = true;
            for (AqlCompiledIndexDecl idx : idxList) {
                List<Pair<String, Integer>> psi = foundIdxExprs.get(idx);
                if (psi == null) {
                    psi = new ArrayList<Pair<String, Integer>>();
                    foundIdxExprs.put(idx, psi);
                }
                int varPos = 0;
                for (LogicalVariable v : comparedVars) {
                    if (v == var) {
                        psi.add(new Pair<String, Integer>(fieldName, varPos));
                    }
                    varPos++;
                }
            }
        }
        return foundVar;
    }

    protected static List<Object> primaryIndexTypes(AqlCompiledMetadataDeclarations metadata,
            AqlCompiledDatasetDecl ddecl, IAType itemType) {
        List<Object> types = new ArrayList<Object>();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(ddecl);
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : partitioningFunctions) {
            types.add(t.third);
        }
        types.add(itemType);
        return types;
    }

}
