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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.util.OptionalBoolean;

public class FakeIndex extends Index {
    private static final long serialVersionUID = 8100544760363679761L;

    public FakeIndex(String databaseName, DataverseName dataverseName, String datasetName, String indexName,
            List<List<String>> fieldNames, boolean isPrimary) {
        super(databaseName, dataverseName, datasetName, indexName, DatasetConfig.IndexType.BTREE,
                new ValueIndexDetails(fieldNames, Collections.nCopies(fieldNames.size(), 0),
                        Collections.nCopies(fieldNames.size(), BuiltinType.ANY), false, OptionalBoolean.FALSE(),
                        OptionalBoolean.empty(), null, null, null)

                , false, isPrimary, MetadataUtil.PENDING_NO_OP, Creator.DEFAULT_CREATOR);
    }

    public FakeIndex(String databaseName, DataverseName dataverseName, String datasetName, String indexName,
            ArrayIndexElement arrayIndexElement) {
        super(databaseName, dataverseName, datasetName, indexName, DatasetConfig.IndexType.ARRAY,
                new ArrayIndexDetails(Collections.singletonList(arrayIndexElement), false), false, false,
                MetadataUtil.PENDING_NO_OP, Creator.DEFAULT_CREATOR);

    }

}
