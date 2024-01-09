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

import static org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression.TRUE;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.visitor.ExpectedSchemaMergerVisitor;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.AllVariablesSubstituteVisitor;

/**
 *
 */
public class ConsolidateProjectionAndFilterExpressionsProcessor extends AbstractPushdownProcessor {
    private final AllVariablesSubstituteVisitor substituteVisitor;
    private final ExpectedSchemaMergerVisitor schemaMergerVisitor;

    public ConsolidateProjectionAndFilterExpressionsProcessor(PushdownContext pushdownContext,
            IOptimizationContext context) {
        super(pushdownContext, context);
        substituteVisitor = new AllVariablesSubstituteVisitor();
        schemaMergerVisitor = new ExpectedSchemaMergerVisitor();
    }

    @Override
    public boolean process() throws AlgebricksException {
        boolean changed = false;
        Collection<List<ScanDefineDescriptor>> scanDescriptors =
                pushdownContext.getDatasetToScanDefinitionDescriptors().values();
        for (List<ScanDefineDescriptor> descriptors : scanDescriptors) {
            changed |= consolidate(descriptors);
        }
        return changed;
    }

    private boolean consolidate(List<ScanDefineDescriptor> scanDefineDescriptors) throws AlgebricksException {
        if (scanDefineDescriptors.size() <= 1) {
            return false;
        }
        Map<ILogicalExpression, ARecordType> paths = new HashMap<>();
        Map<String, FunctionCallInformation> sourceInformationMap = new HashMap<>();
        ILogicalExpression rangeFilterExpr = TRUE;
        ILogicalExpression filterExpr = TRUE;
        RootExpectedSchemaNode mergedRoot = null;
        RootExpectedSchemaNode mergedMetaRoot = null;

        // First combine filters and projected fields
        for (ScanDefineDescriptor descriptor : scanDefineDescriptors) {
            Map<ILogicalExpression, ARecordType> scanPaths = descriptor.getFilterPaths();
            paths.putAll(scanPaths);

            sourceInformationMap.putAll(descriptor.getPathLocations());

            rangeFilterExpr = or(rangeFilterExpr, descriptor.getRangeFilterExpression());

            filterExpr = or(filterExpr, descriptor.getFilterExpression());

            mergedRoot = schemaMergerVisitor.merge(mergedRoot, descriptor.getRecordNode());
            mergedMetaRoot = schemaMergerVisitor.merge(mergedMetaRoot, descriptor.getMetaNode());
        }

        // Set the consolidated pushdown information
        for (ScanDefineDescriptor descriptor : scanDefineDescriptors) {
            Map<ILogicalExpression, ARecordType> descriptorPaths = descriptor.getFilterPaths();
            LogicalVariable scanVariable = descriptor.getVariable();
            descriptorPaths.clear();
            cloneAndSubstituteVariable(scanVariable, paths, descriptorPaths);

            Map<String, FunctionCallInformation> decsriptorSourceInformationMap = descriptor.getPathLocations();
            decsriptorSourceInformationMap.putAll(sourceInformationMap);

            descriptor.setRangeFilterExpression(cloneAndSubstituteVariable(scanVariable, rangeFilterExpr));

            descriptor.setFilterExpression(cloneAndSubstituteVariable(scanVariable, filterExpr));
            descriptor.setRecordNode(mergedRoot);
            descriptor.setMetaNode(mergedMetaRoot);
        }

        return true;
    }

    private AbstractFunctionCallExpression or(ILogicalExpression mergedExpr, ILogicalExpression expr) {
        if (expr == null || mergedExpr == null) {
            return null;
        }
        AbstractFunctionCallExpression orExpr = mergedExpr == TRUE ? null : (AbstractFunctionCallExpression) mergedExpr;
        if (orExpr == null) {
            MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
            IFunctionInfo fInfo = metadataProvider.lookupFunction(BuiltinFunctions.OR);
            orExpr = new ScalarFunctionCallExpression(fInfo);
        }
        orExpr.getArguments().add(new MutableObject<>(expr));
        return orExpr;
    }

    private void cloneAndSubstituteVariable(LogicalVariable scanVariable, Map<ILogicalExpression, ARecordType> src,
            Map<ILogicalExpression, ARecordType> dest) throws AlgebricksException {
        for (Map.Entry<ILogicalExpression, ARecordType> srcEntry : src.entrySet()) {
            ILogicalExpression substitutedExpr = srcEntry.getKey().cloneExpression();
            substitutedExpr.accept(substituteVisitor, scanVariable);
            dest.put(substitutedExpr, srcEntry.getValue());
        }
    }

    private ILogicalExpression cloneAndSubstituteVariable(LogicalVariable scanVariable, ILogicalExpression expression)
            throws AlgebricksException {
        if (expression == null) {
            return null;
        }

        ILogicalExpression clonedExpr = expression.cloneExpression();
        clonedExpr.accept(substituteVisitor, scanVariable);
        return clonedExpr;
    }

}
