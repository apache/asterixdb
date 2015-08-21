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
package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class SqlSumAggregateFunction extends AbstractSumAggregateFunction {
    private final boolean isLocalAgg;
    
    public SqlSumAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider, boolean isLocalAgg)
            throws AlgebricksException {
        super(args, provider);
        this.isLocalAgg = isLocalAgg;
    }

    @Override
    protected void processNull() {
    }

    @Override
    protected void processSystemNull() throws AlgebricksException {
        // For global aggregates simply ignore system null here,
        // but if all input value are system null, then we should return
        // null in finish().
        if (isLocalAgg) {
            throw new AlgebricksException("Type SYSTEM_NULL encountered in local aggregate.");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void finishSystemNull() throws IOException {
        // Empty stream. For local agg return system null. For global agg return null.
        if (isLocalAgg) {
            out.writeByte(ATypeTag.SYSTEM_NULL.serialize());
        } else {
            serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
            serde.serialize(ANull.NULL, out);
        }
    }
}
