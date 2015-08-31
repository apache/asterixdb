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
package org.apache.asterix.aql.expression;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.aql.literal.StringLiteral;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateChannelStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final FunctionSignature function;
    private final CallExpr period;
    private String duration;
    private InsertStatement channelResultsInsertQuery;
    private String subscriptionsTableName;
    private String resultsTableName;

    public CreateChannelStatement(Identifier dataverseName, Identifier channelName, FunctionSignature function,
            Expression period) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.function = function;
        this.period = (CallExpr) period;
        this.duration = "";
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public String getResultsName() {
        return resultsTableName;
    }

    public String getSubscriptionsName() {
        return subscriptionsTableName;
    }

    public String getDuration() {
        return duration;
    }

    public FunctionSignature getFunction() {
        return function;
    }

    public Expression getPeriod() {
        return period;
    }

    public InsertStatement getChannelResultsInsertQuery() {
        return channelResultsInsertQuery;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_CHANNEL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreateChannelStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public void initialize(MetadataTransactionContext mdTxnCtx, String subscriptionsTableName, String resultsTableName)
            throws MetadataException, HyracksDataException {
        Function lookup = MetadataManager.INSTANCE.getFunction(mdTxnCtx, function);

        if (lookup == null) {
            throw new MetadataException(" Unknown function " + function.getName());
        }

        if (!period.getFunctionSignature().getName().equals("duration")) {
            throw new MetadataException("Expected argument period as a duration, but got "
                    + period.getFunctionSignature().getName() + ".");
        }
        duration = ((StringLiteral) ((LiteralExpr) period.getExprList().get(0)).getValue()).getValue();
        IValueParser durationParser = ADurationParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        durationParser.parse(duration.toCharArray(), 0, duration.toCharArray().length, outputStream);
        this.resultsTableName = resultsTableName;
        this.subscriptionsTableName = subscriptionsTableName;

    }
}