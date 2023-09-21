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

package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.fulltext.AbstractFullTextFilterDescriptor;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;

import com.google.common.collect.ImmutableList;

public class FullTextUtil {
    public static IFullTextConfigEvaluatorFactory fetchFilterAndCreateConfigEvaluator(MetadataProvider metadataProvider,
            String database, DataverseName dataverseName, String configName) throws AlgebricksException {
        FullTextConfigDescriptor configDescriptor =
                metadataProvider.findFullTextConfig(database, dataverseName, configName).getFullTextConfig();

        ImmutableList.Builder<AbstractFullTextFilterDescriptor> filterDescriptorsBuilder = ImmutableList.builder();
        for (String filterName : configDescriptor.getFilterNames()) {
            filterDescriptorsBuilder
                    .add(metadataProvider.findFullTextFilter(database, dataverseName, filterName).getFullTextFilter());
        }

        return configDescriptor.createEvaluatorFactory(filterDescriptorsBuilder.build());
    }

}
