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
package org.apache.asterix.optimizer.rules;

import java.util.Set;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.PushdownProcessorsExecutor;
import org.apache.asterix.optimizer.rules.pushdown.processor.ColumnFilterPushdownProcessor;
import org.apache.asterix.optimizer.rules.pushdown.processor.ColumnRangeFilterPushdownProcessor;
import org.apache.asterix.optimizer.rules.pushdown.processor.ColumnValueAccessPushdownProcessor;
import org.apache.asterix.optimizer.rules.pushdown.processor.ConsolidateProjectionAndFilterExpressionsProcessor;
import org.apache.asterix.optimizer.rules.pushdown.processor.ExternalDatasetFilterPushdownProcessor;
import org.apache.asterix.optimizer.rules.pushdown.processor.InlineAndNormalizeFilterExpressionsProcessor;
import org.apache.asterix.optimizer.rules.pushdown.visitor.PushdownOperatorVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

/**
 * Pushes value access expressions to datasets' scans (if they permit) to minimize the size of the record.
 * This rule currently does not remove the value access expression. Instead, it adds the requested field names to
 * data-scan operator to produce records that only contain the requested values. The rule also pushes down filter
 * expressions to data-scans if scanned datasets permit. This rule does not change the plan's structure after firing.
 * Example:
 * Before plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$0, $$r] <- ColumnDataverse.ColumnDataset
 * <p>
 * After plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$0, $$r] <- ColumnDataverse.ColumnDataset
 * project ({personalInfo:{age: any},salary: any})
 * filter on: and(gt($r.getField("personalInfo").getField("age"), 20), gt($$r.getField("salary"), 70000))
 * range-filter on: and(gt($r.getField("personalInfo").getField("age"), 20), gt($$r.getField("salary"), 70000))
 * <p>
 * The resulting record $$r will be {"personalInfo":{"age": *AGE*}, "salary": *SALARY*}
 * and other fields will not be included in $$r.
 */
public class PushValueAccessAndFilterDownRule implements IAlgebraicRewriteRule {
    private boolean run = true;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // TODO this should be revised after introducing the proper compiler flags
        if (!context.getPhysicalOptimizationConfig().isExternalFieldPushdown() || !run) {
            //The rule was fired, or value access pushdown is disabled
            return false;
        }

        /*
         * Only run this rewrite rule once and only if the plan contains a data-scan on a dataset that
         * supports value-access, filter, and/or range-filter.
         */
        run = shouldRun(context);
        boolean changed = false;
        if (run) {
            // Context holds all the necessary information to perform pushdowns
            PushdownContext pushdownContext = new PushdownContext(context);
            // Compute all the necessary pushdown information and performs inter-operator pushdown optimizations
            PushdownOperatorVisitor pushdownInfoComputer = new PushdownOperatorVisitor(pushdownContext, context);
            opRef.getValue().accept(pushdownInfoComputer, null);
            // Execute several optimization passes to perform the pushdown
            PushdownProcessorsExecutor pushdownProcessorsExecutor = new PushdownProcessorsExecutor();
            addProcessors(pushdownProcessorsExecutor, pushdownContext, context);
            changed = pushdownProcessorsExecutor.execute();
            pushdownProcessorsExecutor.finalizePushdown(pushdownContext, context);
            run = false;
        }
        return changed;
    }

    private void addProcessors(PushdownProcessorsExecutor pushdownProcessorsExecutor, PushdownContext pushdownContext,
            IOptimizationContext context) {
        // Performs value-access pushdowns
        pushdownProcessorsExecutor.add(new ColumnValueAccessPushdownProcessor(pushdownContext, context));
        if (context.getPhysicalOptimizationConfig().isColumnFilterEnabled()) {
            // Performs filter pushdowns
            pushdownProcessorsExecutor.add(new ColumnFilterPushdownProcessor(pushdownContext, context));
            // Performs range-filter pushdowns
            pushdownProcessorsExecutor.add(new ColumnRangeFilterPushdownProcessor(pushdownContext, context));
        }
        // Performs prefix pushdowns
        pushdownProcessorsExecutor.add(new ExternalDatasetFilterPushdownProcessor(pushdownContext, context));
        pushdownProcessorsExecutor
                .add(new ConsolidateProjectionAndFilterExpressionsProcessor(pushdownContext, context));
        // Inlines AND/OR expression (must be last to run)
        pushdownProcessorsExecutor.add(new InlineAndNormalizeFilterExpressionsProcessor(pushdownContext, context));
    }

    /**
     * Check whether the plan contains a dataset that supports pushdown
     *
     * @param context optimization context
     * @return true if the plan contains such dataset, false otherwise
     */
    private boolean shouldRun(IOptimizationContext context) throws AlgebricksException {
        ObjectSet<Int2ObjectMap.Entry<Set<DataSource>>> entrySet =
                ((AsterixOptimizationContext) context).getDataSourceMap().int2ObjectEntrySet();
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        for (Int2ObjectMap.Entry<Set<DataSource>> dataSources : entrySet) {
            for (DataSource dataSource : dataSources.getValue()) {
                if (supportPushdown(metadataProvider, dataSource)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean supportPushdown(MetadataProvider metadataProvider, DataSource dataSource)
            throws AlgebricksException {
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        String database = dataSource.getId().getDatabaseName();
        Dataset dataset = metadataProvider.findDataset(database, dataverse, datasetName);

        return dataset != null && (DatasetUtil.isFieldAccessPushdownSupported(dataset)
                || DatasetUtil.isFilterPushdownSupported(dataset)
                || DatasetUtil.isRangeFilterPushdownSupported(dataset));
    }
}
