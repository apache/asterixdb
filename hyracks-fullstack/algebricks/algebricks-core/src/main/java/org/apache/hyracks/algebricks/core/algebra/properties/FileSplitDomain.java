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
package org.apache.hyracks.algebricks.core.algebra.properties;

import org.apache.hyracks.api.io.FileSplit;

public class FileSplitDomain implements INodeDomain {

    private FileSplit[] splits;

    public FileSplitDomain(FileSplit[] splits) {
        this.splits = splits;
    }

    @Override
    public Integer cardinality() {
        return splits.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FileSplitDomain[");
        boolean fst = true;
        for (FileSplit fs : splits) {
            if (fst) {
                fst = false;
            } else {
                sb.append(", ");
            }
            sb.append(fs.getNodeName() + ":" + fs.getPath());
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public boolean sameAs(INodeDomain domain) {
        if (!(domain instanceof FileSplitDomain)) {
            return false;
        }
        FileSplitDomain fsd = (FileSplitDomain) domain;
        if (fsd.splits.length != splits.length) {
            return false;
        }
        // conservative approach...
        for (int i = 0; i < splits.length; i++) {
            if (!ncEq(splits[i], fsd.splits[i])) {
                return false;
            }
        }

        return true;
    }

    private boolean ncEq(FileSplit fs1, FileSplit fs2) {
        return fs1.getNodeName().equals(fs2.getNodeName());
    }

}
