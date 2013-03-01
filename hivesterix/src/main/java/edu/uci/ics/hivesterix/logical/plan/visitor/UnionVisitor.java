package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class UnionVisitor extends DefaultVisitor {

	List<Mutable<ILogicalOperator>> parents = new ArrayList<Mutable<ILogicalOperator>>();

	@Override
	public Mutable<ILogicalOperator> visit(UnionOperator operator,
			Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t)
			throws AlgebricksException {

		parents.add(AlgebricksParentOperator);
		if (operator.getParentOperators().size() > parents.size()) {
			return null;
		}

		List<LogicalVariable> leftVars = new ArrayList<LogicalVariable>();
		List<LogicalVariable> rightVars = new ArrayList<LogicalVariable>();

		VariableUtilities.getUsedVariables(parents.get(0).getValue(), leftVars);
		VariableUtilities
				.getUsedVariables(parents.get(1).getValue(), rightVars);

		List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> triples = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();
		List<LogicalVariable> unionVars = new ArrayList<LogicalVariable>();

		for (int i = 0; i < leftVars.size(); i++) {
			LogicalVariable unionVar = t.getVariable(leftVars.get(i).getId()
					+ "union" + AlgebricksParentOperator.hashCode(),
					TypeInfoFactory.unknownTypeInfo);
			unionVars.add(unionVar);
			Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple = new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(
					leftVars.get(i), rightVars.get(i), unionVar);
			t.replaceVariable(leftVars.get(i), unionVar);
			t.replaceVariable(rightVars.get(i), unionVar);
			triples.add(triple);
		}
		ILogicalOperator currentOperator = new edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator(
				triples);
		for (Mutable<ILogicalOperator> parent : parents)
			currentOperator.getInputs().add(parent);

		t.rewriteOperatorOutputSchema(unionVars, operator);
		parents.clear();
		return new MutableObject<ILogicalOperator>(currentOperator);
	}

}
