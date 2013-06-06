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
package edu.uci.ics.asterix.aql.expression.visitor;

import edu.uci.ics.asterix.aql.expression.JoinClause;
import edu.uci.ics.asterix.aql.expression.MetaVariableClause;
import edu.uci.ics.asterix.aql.expression.MetaVariableExpr;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IAqlPlusExpressionVisitor<R, T> extends IAqlExpressionVisitor<R, T> {
    R visitJoinClause(JoinClause c, T arg) throws AsterixException;

    R visitMetaVariableClause(MetaVariableClause c, T arg) throws AsterixException;

    R visitMetaVariableExpr(MetaVariableExpr v, T arg) throws AsterixException;
}
