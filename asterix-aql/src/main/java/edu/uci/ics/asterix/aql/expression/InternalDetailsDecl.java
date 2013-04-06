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
package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;

public class InternalDetailsDecl implements IDatasetDetailsDecl {
    private final Identifier nodegroupName;
    private final List<String> partitioningExprs;

    public InternalDetailsDecl(Identifier nodeGroupName, List<String> partitioningExpr) {
        this.nodegroupName = nodeGroupName == null ? new Identifier(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)
                : nodeGroupName;
        this.partitioningExprs = partitioningExpr;
    }

    public List<String> getPartitioningExprs() {
        return partitioningExprs;
    }

    public Identifier getNodegroupName() {
        return nodegroupName;
    }
}
