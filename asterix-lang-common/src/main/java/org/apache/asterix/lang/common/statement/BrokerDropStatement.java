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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class BrokerDropStatement implements Statement {

    private String brokerName;
    private boolean ifExists;

    public BrokerDropStatement(String brokerName, boolean ifExists) {
        this.brokerName = brokerName;
        this.ifExists = ifExists;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public String getBrokerName() {
        return brokerName;
    }

    @Override
    public Kind getKind() {
        return Kind.DROP_BROKER;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

}