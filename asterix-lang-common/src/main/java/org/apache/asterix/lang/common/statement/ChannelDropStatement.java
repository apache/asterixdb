/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class ChannelDropStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private boolean ifExists;

    public ChannelDropStatement(Identifier dataverseName, Identifier channelName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.channelName = channelName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.DROP_CHANNEL;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDropChannelStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}