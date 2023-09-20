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

package org.apache.asterix.runtime.fulltext;

import java.util.List;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextFilterType;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.StopwordsFullTextFilterEvaluatorFactory;

import com.google.common.collect.ImmutableList;

public class StopwordsFullTextFilterDescriptor extends AbstractFullTextFilterDescriptor {
    private static final long serialVersionUID = 1L;

    public ImmutableList<String> stopwordList;

    public StopwordsFullTextFilterDescriptor(String database, DataverseName dataverseName, String name,
            ImmutableList<String> stopwordList) {
        super(database, dataverseName, name);
        this.stopwordList = stopwordList;
    }

    @Override
    public FullTextFilterType getFilterType() {
        return FullTextFilterType.STOPWORDS;
    }

    public List<String> getStopwordList() {
        return this.stopwordList;
    }

    @Override
    public IFullTextFilterEvaluatorFactory createEvaluatorFactory() {
        return new StopwordsFullTextFilterEvaluatorFactory(name, stopwordList);
    }
}
