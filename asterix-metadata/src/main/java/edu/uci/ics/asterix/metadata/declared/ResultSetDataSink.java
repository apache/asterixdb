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

package edu.uci.ics.asterix.metadata.declared;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ResultSetDomain;

public class ResultSetDataSink implements IDataSink {

    private ResultSetSinkId id;
    private Object[] schemaTypes;

    public ResultSetDataSink(ResultSetSinkId id, Object[] schemaTypes) {
        this.id = id;
        this.schemaTypes = schemaTypes;
    }

    @Override
    public ResultSetSinkId getId() {
        return id;
    }

    @Override
    public Object[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return new RandomPartitioningProperty(new ResultSetDomain());
    }
}
