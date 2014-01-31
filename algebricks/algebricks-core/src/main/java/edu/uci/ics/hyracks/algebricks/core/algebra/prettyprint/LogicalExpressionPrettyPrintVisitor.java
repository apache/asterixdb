/*
 * Copyright 2014 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;


public class LogicalExpressionPrettyPrintVisitor implements ILogicalExpressionVisitor<String, Integer> {

	@Override
	public String visitConstantExpression(ConstantExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}

	@Override
	public String visitVariableReferenceExpression(
			VariableReferenceExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}

	@Override
	public String visitAggregateFunctionCallExpression(
			AggregateFunctionCallExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}

	@Override
	public String visitScalarFunctionCallExpression(
			ScalarFunctionCallExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}

	@Override
	public String visitStatefulFunctionCallExpression(
			StatefulFunctionCallExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}

	@Override
	public String visitUnnestingFunctionCallExpression(
			UnnestingFunctionCallExpression expr, Integer indent)
			throws AlgebricksException {
		return expr.toString();
	}
}

