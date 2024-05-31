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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hyracks.data.std.api.IValueReference;

import it.unimi.dsi.fastutil.bytes.Byte2ObjectArrayMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;

class TrieNode {
    public static final int NOT_FOUND_INDEX = -1;

    private Byte2ObjectMap<TrieNode> children;
    private boolean isEndOfField;
    private int index;
    private int start; // includes the edges' byte
    private int length; // includes the edges' byte
    private int bytesToStoreLength;

    TrieNode() {
        children = new Byte2ObjectArrayMap<>();
        index = NOT_FOUND_INDEX;
    }

    TrieNode(Byte2ObjectMap<TrieNode> children) {
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

    public boolean containsKey(byte key) {
        return children.containsKey(key);
    }

    public TrieNode getChild(byte key) {
        return children.get(key);
    }

    public void putChild(byte key, TrieNode child) {
        children.put(key, child);
    }

    public Byte2ObjectMap<TrieNode> getChildren() {
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
        children = new Byte2ObjectArrayMap<>();
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
        // serialize fields
        out.writeBoolean(isEndOfField);
        out.writeInt(index);
        out.writeInt(start);
        out.writeInt(length);
        out.writeInt(bytesToStoreLength);

        out.writeInt(children.size());
        for (Map.Entry<Byte, TrieNode> entry : children.byte2ObjectEntrySet()) {
            out.writeByte(entry.getKey());
            entry.getValue().serialize(out);
        }
    }

    public static TrieNode deserialize(DataInput in) throws IOException {
        TrieNode node = new TrieNode();
        node.isEndOfField = in.readBoolean();
        node.index = in.readInt();
        node.start = in.readInt();
        node.length = in.readInt();
        node.bytesToStoreLength = in.readInt();

        int childrenSize = in.readInt();
        for (int i = 0; i < childrenSize; i++) {
            byte b = in.readByte();
            node.children.put(b, TrieNode.deserialize(in));
        }
        return node;
    }
}
