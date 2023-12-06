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
package org.apache.asterix.optimizer.rules.pushdown.processor;

import java.util.List;

import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.visitor.ExpressionValueAccessPushdownVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

/**
 * Computes the expected schema for columnar datasets (whether internal or external). The expected schema is then
 * used to project only the values that were requested by the query.
 */
public class ColumnValueAccessPushdownProcessor extends AbstractPushdownProcessor {
    private final ExpectedSchemaBuilder builder;
    private final ExpressionValueAccessPushdownVisitor expressionVisitor;

    public ColumnValueAccessPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
        builder = new ExpectedSchemaBuilder();
        expressionVisitor = new ExpressionValueAccessPushdownVisitor(builder);
    }

    @Override
    public boolean process() throws AlgebricksException {
        List<ScanDefineDescriptor> scanDefineDescriptors = pushdownContext.getRegisteredScans();
        boolean changed = false;
        for (ScanDefineDescriptor scanDefineDescriptor : scanDefineDescriptors) {
            if (!DatasetUtil.isFieldAccessPushdownSupported(scanDefineDescriptor.getDataset())) {
                continue;
            }
            pushdownFieldAccessForDataset(scanDefineDescriptor);
            RootExpectedSchemaNode root = (RootExpectedSchemaNode) builder.getNode(scanDefineDescriptor.getVariable());
            scanDefineDescriptor.setRecordNode(root);
            changed |= !root.isAllFields();
            if (scanDefineDescriptor.hasMeta()) {
                RootExpectedSchemaNode metaRoot =
                        (RootExpectedSchemaNode) builder.getNode(scanDefineDescriptor.getMetaRecordVariable());
                changed |= !metaRoot.isAllFields();
                scanDefineDescriptor.setMetaNode(metaRoot);
            }
        }
        return changed;
    }

    private void pushdownFieldAccessForDataset(ScanDefineDescriptor scanDefineDescriptor) throws AlgebricksException {
        builder.registerRoot(scanDefineDescriptor.getVariable(), scanDefineDescriptor.getRecordNode());
        if (scanDefineDescriptor.hasMeta()) {
            builder.registerRoot(scanDefineDescriptor.getMetaRecordVariable(), scanDefineDescriptor.getMetaNode());
        }
        pushdownFieldAccess(scanDefineDescriptor);
    }

    private void pushdownFieldAccess(DefineDescriptor defineDescriptor) throws AlgebricksException {
        List<UseDescriptor> useDescriptors = pushdownContext.getUseDescriptors(defineDescriptor);
        for (UseDescriptor useDescriptor : useDescriptors) {
            LogicalVariable producedVariable = useDescriptor.getProducedVariable();
            ILogicalOperator op = useDescriptor.getOperator();
            IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(op, context);
            expressionVisitor.transform(useDescriptor.getExpression(), producedVariable, typeEnv);
        }

        /*
         * Two loops are needed as we need first to build the schemas for all useDescriptors expressions and then
         * follow through (if the useDescriptor expression was assigned to a variable). In other words, the traversal
         * of the expression tree has to be BFS and not DFS to prevent building a schema for undeclared variable.
         * 'Undeclared variable' means we don't have a schema for a variable as we didn't visit it.
         */
        for (UseDescriptor useDescriptor : useDescriptors) {
            DefineDescriptor nextDefineDescriptor = pushdownContext.getDefineDescriptor(useDescriptor);
            if (nextDefineDescriptor != null) {
                pushdownFieldAccess(nextDefineDescriptor);
            }
        }
    }
}
