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
package org.apache.asterix.common.feeds;

import java.io.Serializable;

import org.apache.asterix.common.feeds.ActiveId.ActiveObjectType;

/**
 * A unique identifier for a feed connection. A feed connection is an instance of a data feed that is flowing into a dataset.
 */

public class FeedConnectionId extends ActiveJobId implements Serializable {

    private static final long serialVersionUID = 1L;

    final String datasetName;

    public FeedConnectionId(ActiveId activeId, String datasetName) {
        super(activeId);
        this.datasetName = datasetName;
    }

    public FeedConnectionId(String dataverse, String feedName, String datasetName) {
        super(new ActiveId(dataverse, feedName, ActiveObjectType.FEED));
        this.datasetName = datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedConnectionId)) {
            return false;
        }
        if (this == o
                || (((FeedConnectionId) o).getActiveId().equals(activeId) && ((FeedConnectionId) o).getDatasetName()
                        .equals(datasetName))) {
            return true;
        }
        return false;
    }

    public String getDatasetName() {
        return datasetName;
    }
}
