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
package org.apache.asterix.om.dictionary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import it.unimi.dsi.fastutil.objects.ObjectArrays;

final class ByteToNodeMap {
    private static final TrieNode[] EMPTY = new TrieNode[0];
    private TrieNode[] children;
    private int numberOfChildren;

    ByteToNodeMap() {
        children = EMPTY;
        numberOfChildren = 0;
    }

    private ByteToNodeMap(TrieNode[] children, int numberOfChildren) {
        this.children = children;
        this.numberOfChildren = numberOfChildren;
    }

    void put(byte key, TrieNode node) {
        int index = Byte.toUnsignedInt(key);
        ensure(index);
        children[index] = node;
        numberOfChildren++;
    }

    TrieNode get(byte key) {
        int index = Byte.toUnsignedInt(key);
        if (index < children.length) {
            return children[index];
        }

        return null;
    }

    private void ensure(int index) {
        if (index >= children.length) {
            children = ObjectArrays.grow(children, index + 1, children.length);
        }
    }

    void addAllChildren(Collection<TrieNode> collection) {
        int addedChildren = 0;
        for (int i = 0; i < children.length && addedChildren < numberOfChildren; i++) {
            TrieNode child = children[i];
            if (child != null) {
                collection.add(children[i]);
                addedChildren++;
            }
        }
    }

    void serialize(DataOutput out) throws IOException {
        out.writeInt(numberOfChildren);
        out.writeInt(children.length);
        int addedChildren = 0;
        for (int i = 0; i < children.length && addedChildren < numberOfChildren; i++) {
            TrieNode child = children[i];
            if (child != null) {
                out.writeInt(i);
                child.serialize(out);
                addedChildren++;
            }
        }
    }

    static ByteToNodeMap deserialize(DataInput in) throws IOException {
        int numberOfChildren = in.readInt();
        int length = in.readInt();
        TrieNode[] children = length == 0 ? EMPTY : new TrieNode[length];
        for (int i = 0; i < numberOfChildren; i++) {
            int index = in.readInt();
            children[index] = TrieNode.deserialize(in);
        }

        return new ByteToNodeMap(children, numberOfChildren);
    }
}
