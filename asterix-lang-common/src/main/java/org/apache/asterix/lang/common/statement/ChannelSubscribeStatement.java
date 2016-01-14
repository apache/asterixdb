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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class ChannelSubscribeStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final String brokerName;
    private final List<Expression> argList;
    private final int varCounter;

    public ChannelSubscribeStatement(Identifier dataverseName, Identifier channelName, List<Expression> argList,
            int varCounter, String brokerName) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.brokerName = brokerName;
        this.argList = argList;
        this.varCounter = varCounter;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public List<Expression> getArgList() {
        return argList;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.SUBSCRIBE_CHANNEL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

}