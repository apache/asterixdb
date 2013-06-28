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
package edu.uci.ics.hyracks.algebricks.examples.piglet.metadata;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class PigletFileDataSink implements IDataSink {
    private String file;

    private FileSplit[] fileSplits;

    private IPartitioningProperty partProp;

    public PigletFileDataSink(String file) {
        this.file = file;
        fileSplits = FileSplitUtils.parseFileSplits(file);
        partProp = new RandomPartitioningProperty(new FileSplitDomain(fileSplits));
    }

    @Override
    public Object getId() {
        return file;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

    @Override
    public Object[] getSchemaTypes() {
        return null;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return partProp;
    }
}