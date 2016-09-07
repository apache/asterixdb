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
package org.apache.asterix.metadata.api;

import java.io.Serializable;

import org.apache.asterix.common.api.ExtensionId;
import org.apache.commons.lang3.ObjectUtils;

public class ExtensionMetadataDatasetId implements Serializable {

    private static final long serialVersionUID = 1L;
    private final ExtensionId extensionId;
    private final String datasetName;

    public ExtensionMetadataDatasetId(ExtensionId extensionId, String datasetName) {
        this.extensionId = extensionId;
        this.datasetName = datasetName;
    }

    public ExtensionId getExtensionId() {
        return extensionId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ExtensionMetadataDatasetId) {
            ExtensionMetadataDatasetId otherId = (ExtensionMetadataDatasetId) o;
            return ObjectUtils.equals(extensionId, otherId.getExtensionId())
                    && ObjectUtils.equals(datasetName, otherId.getDatasetName());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(datasetName, extensionId);
    }
}
