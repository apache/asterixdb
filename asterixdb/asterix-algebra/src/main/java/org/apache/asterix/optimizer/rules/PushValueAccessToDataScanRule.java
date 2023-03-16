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

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.asterix.optimizer.rules.pushdown.OperatorValueAccessPushdownVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

/**
 * Pushes value access expressions to the external dataset scan to minimize the size of the record.
 * This rule currently does not remove the value access expression. Instead, it adds the requested field names to
 * external dataset details to produce records that only contain the requested values. Thus, no changes would occur
 * to the plan's structure after firing this rule.
 * Example:
 * Before plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$r] <- ParquetDataverse.ParquetDataset
 * <p>
 * After plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$r] <- ParquetDataverse.ParquetDataset project ({personalInfo:{age: VALUE},salary:VALUE})
 * <p>
 * The resulting record $$r will be {"personalInfo":{"age": *AGE*}, "salary": *SALARY*}
 * and other fields will not be included in $$r.
 */
public class PushValueAccessToDataScanRule implements IAlgebraicRewriteRule {
    //Initially, assume we need to run the rule
    private boolean run = true;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().isExternalFieldPushdown() || !run) {
            //The rule was fired, or value access pushdown is disabled
            return false;
        }

        /*
         * Only run the rewrite rule once and only if the plan contains a data-scan on a dataset that
         * support value access pushdown.
         */
        run = shouldRun(context);
        if (run) {
            run = false;
            OperatorValueAccessPushdownVisitor visitor = new OperatorValueAccessPushdownVisitor(context);
            opRef.getValue().accept(visitor, null);
            visitor.finish();
        }

        //This rule does not do any actual structural changes to the plan
        return false;
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
        Dataset dataset = metadataProvider.findDataset(dataverse, datasetName);

        return dataset != null && ((dataset.getDatasetType() == DatasetConfig.DatasetType.EXTERNAL && ExternalDataUtils
                .supportsPushdown(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties()))
                || dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN);
    }
}
