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
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class DisconnectFeedStatement extends AbstractStatement {

    private final Identifier dataverseName;
    private final Identifier feedName;
    private final Identifier datasetName;

    public DisconnectFeedStatement(Identifier dataverseName, Identifier feedName, Identifier datasetName) {
        this.feedName = feedName;
        this.datasetName = datasetName;
        this.dataverseName = dataverseName;
    }

    public DisconnectFeedStatement(Pair<Identifier, Identifier> feedNameComponent,
            Pair<Identifier, Identifier> datasetNameComponent) {
        if (feedNameComponent.first != null && datasetNameComponent.first != null
                && !feedNameComponent.first.getValue().equals(datasetNameComponent.first.getValue())) {
            throw new IllegalArgumentException("Dataverse for source feed and target dataset do not match");
        }
        this.dataverseName = feedNameComponent.first != null ? feedNameComponent.first
                : datasetNameComponent.first != null ? datasetNameComponent.first : null;
        this.datasetName = datasetNameComponent.second;
        this.feedName = feedNameComponent.second;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getFeedName() {
        return feedName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.DISCONNECT_FEED;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public String toString() {
        return "disconnect feed " + feedName + " from " + datasetName;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

}
