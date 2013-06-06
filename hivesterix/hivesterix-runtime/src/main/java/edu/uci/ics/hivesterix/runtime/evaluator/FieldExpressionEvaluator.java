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
package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeFieldEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class FieldExpressionEvaluator extends AbstractExpressionEvaluator {

    public FieldExpressionEvaluator(ExprNodeFieldDesc expr, ObjectInspector oi, IDataOutputProvider output)
            throws AlgebricksException {
        super(new ExprNodeFieldEvaluator(expr), oi, output);
    }

}