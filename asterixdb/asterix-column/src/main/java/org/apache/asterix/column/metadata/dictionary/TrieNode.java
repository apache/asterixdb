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

package org.apache.asterix.column.metadata.dictionary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IValueReference;

class TrieNode {
    public static final int NOT_FOUND_INDEX = -1;

    private ByteToNodeMap children;
    private boolean isEndOfField;
    private int index;
    private int start; // includes the edges' byte
    private int length; // includes the edges' byte
    private int bytesToStoreLength;

    TrieNode() {
        this.children = new ByteToNodeMap();
        index = NOT_FOUND_INDEX;
    }

    TrieNode(ByteToNodeMap children) {
        this.children = children;
        index = NOT_FOUND_INDEX;
    }

    public void setIndex(int index, int start, int length, int bytesToStoreLength) {
        this.index = index;
        this.start = start;
        this.length = length;
        this.bytesToStoreLength = bytesToStoreLength;
    }

    public void setIsEndOfField(boolean isEndOfField) {
        this.isEndOfField = isEndOfField;
    }

    public TrieNode getChild(byte key) {
        return children.get(key);
    }

    public void putChild(byte key, TrieNode child) {
        children.put(key, child);
    }

    public ByteToNodeMap getChildren() {
        return children;
    }

    public int getIndex() {
        return index;
    }

    public int getStart() {
        return start;
    }

    public int getLength() {
        return length;
    }

    public int getBytesToStoreLength() {
        return bytesToStoreLength;
    }

    public boolean isEndOfField() {
        return isEndOfField;
    }

    public void reset() {
        // since this object went to the new node.
        children = new ByteToNodeMap();
    }

    public void split(IValueReference fieldName, int splitIndex) {
        byte[] storedFieldBytes = fieldName.getByteArray();
        byte splitByte = storedFieldBytes[start + splitIndex];
        // something to be split, have to create a new node
        // and do the linking.
        TrieNode childNode = new TrieNode(children);

        int leftToSplit = length - splitIndex;
        childNode.setIndex(index, start + splitIndex, leftToSplit, bytesToStoreLength);
        childNode.setIsEndOfField(isEndOfField);
        // update the current search node
        // new length would be 'c'
        reset();
        setIndex(index, start, splitIndex, bytesToStoreLength);
        putChild(splitByte, childNode);
        // since there was a split in searchNode, hence isEndOfField will be false.
        setIsEndOfField(false);
    }

    public void serialize(DataOutput out) throws IOException {
        // Serialize child first
        children.serialize(out);
        // serialize fields
        out.writeBoolean(isEndOfField);
        out.writeInt(index);
        out.writeInt(start);
        out.writeInt(length);
        out.writeInt(bytesToStoreLength);
    }

    public static TrieNode deserialize(DataInput in) throws IOException {
        ByteToNodeMap children = ByteToNodeMap.deserialize(in);
        TrieNode node = new TrieNode(children);
        node.isEndOfField = in.readBoolean();
        node.index = in.readInt();
        node.start = in.readInt();
        node.length = in.readInt();
        node.bytesToStoreLength = in.readInt();

        return node;
    }
}
