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
package org.apache.asterix.column.metadata;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class PathInfoSerializer {
    private final ArrayBackedValueStorage primaryKeyOutputPathStorage;
    private final ArrayBackedValueStorage pathOutputStorage;
    private final IntList delimiters;
    private int level;

    public PathInfoSerializer() {
        primaryKeyOutputPathStorage = new ArrayBackedValueStorage();
        pathOutputStorage = new ArrayBackedValueStorage();
        delimiters = new IntArrayList();
        level = 0;
    }

    public void reset() {
        primaryKeyOutputPathStorage.reset();
        pathOutputStorage.reset();
    }

    public void enter(AbstractSchemaNestedNode nestedNode) {
        if (nestedNode.isCollection()) {
            delimiters.add(0, level - 1);
        }
        if (nestedNode.isObjectOrCollection()) {
            level++;
        }
    }

    public void exit(AbstractSchemaNestedNode nestedNode) {
        if (nestedNode.isCollection()) {
            delimiters.removeInt(0);
        }
        if (nestedNode.isObjectOrCollection()) {
            level--;
        }
    }

    public void writePathInfo(ATypeTag typeTag, int columnIndex, boolean primaryKey) throws IOException {
        DataOutput output =
                primaryKey ? primaryKeyOutputPathStorage.getDataOutput() : pathOutputStorage.getDataOutput();
        //type tag
        output.write(typeTag.serialize());
        //columnIndex
        output.writeInt(columnIndex);
        //maxLevel
        output.writeInt(primaryKey ? 1 : level);
        //is primary key
        output.writeBoolean(primaryKey);
        //Is collection
        boolean collection = !delimiters.isEmpty();
        output.writeBoolean(collection);
        if (collection) {
            output.writeInt(delimiters.size());
            for (int i = 0; i < delimiters.size(); i++) {
                output.writeInt(delimiters.getInt(i));
            }
        }
    }

    public void serialize(DataOutput output, int numberOfColumns) throws IOException {
        output.writeInt(numberOfColumns);
        output.write(primaryKeyOutputPathStorage.getByteArray(), 0, primaryKeyOutputPathStorage.getLength());
        output.write(pathOutputStorage.getByteArray(), 0, pathOutputStorage.getLength());
    }
}
