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

import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class AqlCompiledFeedDatasetDetails extends
		AqlCompiledInternalDatasetDetails {
	private final String adapter;
	private final Map<String, String> properties;
	private final String functionIdentifier;
	private final String feedState;

	public AqlCompiledFeedDatasetDetails(
			List<String> partitioningExprs,
			List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitionFuns,
			String nodegroupName, AqlCompiledIndexDecl primaryIndex,
			List<AqlCompiledIndexDecl> secondaryIndexes, String adapter,
			Map<String, String> properties, String functionIdentifier,
			String feedState) {
		super(partitioningExprs, partitionFuns, nodegroupName, primaryIndex,
				secondaryIndexes);
		this.adapter = adapter;
		this.properties = properties;
		this.functionIdentifier = functionIdentifier;
		this.feedState = feedState;
	}

	public String getAdapter() {
		return adapter;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public String getFunctionIdentifier() {
		return functionIdentifier;
	}

	public DatasetType getDatasetType() {
		return DatasetType.FEED;
	}

	public String getFeedState() {
		return feedState;
	}

}
