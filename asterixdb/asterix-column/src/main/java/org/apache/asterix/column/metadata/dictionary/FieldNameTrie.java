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

import static org.apache.asterix.column.metadata.dictionary.AbstractFieldNamesDictionary.deserializeFieldNames;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class FieldNameTrie {
    private static final int VERSION = 1;
    private final LookupState lookupState;

    private final List<IValueReference> fieldNames;
    private TrieNode rootNode;

    public FieldNameTrie() {
        this(new ArrayList<>());
    }

    private FieldNameTrie(List<IValueReference> fieldNames) {
        this.fieldNames = fieldNames;
        this.rootNode = new TrieNode();
        lookupState = new LookupState();
    }

    public int insert(IValueReference fieldName) throws HyracksDataException {
        int presentIndex = lookup(fieldName);
        if (presentIndex == TrieNode.NOT_FOUND_INDEX) {
            presentIndex = hookup(FieldNamesTrieDictionary.creatFieldName(fieldName));
        }
        return presentIndex;
    }

    public int lookup(IValueReference fieldName) {
        //noinspection DuplicatedCode
        int len = UTF8StringUtil.getUTFLength(fieldName.getByteArray(), fieldName.getStartOffset());
        int start = fieldName.getStartOffset() + UTF8StringUtil.getNumBytesToStoreLength(len);
        byte[] bytes = fieldName.getByteArray();

        TrieNode searchNode = rootNode;
        TrieNode prevNode = searchNode;

        int byteIndex = start;
        // previousByteIndex should point to the first byte to be compared
        // when inserting the fieldName
        int previousByteIndex = byteIndex;

        int lastIndex = (start + len - 1);
        while (byteIndex <= lastIndex) {
            byte b = bytes[byteIndex];

            TrieNode nextNode = searchNode.getChild(b);
            if (nextNode == null) {
                // saving state in case hookup is requested
                lookupState.setState(prevNode, start, previousByteIndex, len);
                return TrieNode.NOT_FOUND_INDEX;
            }
            // if the node exists, then compare the remaining byte seq.
            prevNode = searchNode;
            searchNode = nextNode;

            if (searchNode.getLength() > 1) { // first byte will be same as byteIndex
                // compare the stored sequence.
                int fieldBytesLeftToCompare = lastIndex - byteIndex + 1;
                // if the stored sequence in node is greater than the input field's
                // byte to compare, then the result won't be there.
                if (fieldBytesLeftToCompare < searchNode.getLength()) {
                    // saving state in case hookup is requested
                    lookupState.setState(prevNode, start, byteIndex, len);
                    return TrieNode.NOT_FOUND_INDEX;
                }

                int c = 0;
                byte[] storedFieldBytes = fieldNames.get(searchNode.getIndex()).getByteArray();
                int storedFieldStart = searchNode.getStart();
                previousByteIndex = byteIndex;
                while (c < searchNode.getLength()) {
                    if (bytes[byteIndex] != storedFieldBytes[storedFieldStart + c]) {
                        // saving state in case hookup is requested
                        // will restart from oldByteIndex, to make logic simpler.
                        // other way could have been to store the splitIndex.
                        lookupState.setState(prevNode, start, previousByteIndex, len);
                        return TrieNode.NOT_FOUND_INDEX;
                    }
                    c++;
                    byteIndex++;
                }
            } else {
                previousByteIndex = byteIndex;
                byteIndex++;
            }
        }

        // saving state in case hookup is requested
        lookupState.setState(prevNode, start, previousByteIndex, len);
        return searchNode.isEndOfField() ? searchNode.getIndex() : TrieNode.NOT_FOUND_INDEX;
    }

    private int hookup(IValueReference fieldName) {
        // since lookup operation always precedes a hookup operation
        // we can use the saved state to start hookup.
        int len = lookupState.getFieldLength();
        TrieNode searchNode = lookupState.getLastNode();

        // resume from the stored node.
        int bytesToStoreLength = UTF8StringUtil.getNumBytesToStoreLength(len);

        int byteIndex = lookupState.getRelativeOffsetFromStart() + bytesToStoreLength;
        byte[] bytes = fieldName.getByteArray();
        int lastIndex = (bytesToStoreLength + len - 1);
        while (byteIndex <= lastIndex) {
            byte b = bytes[byteIndex];
            TrieNode nextNode = searchNode.getChild(b);
            if (nextNode == null) {
                // since there no such node, then create a node, and put
                // rest bytes in the nodes.
                TrieNode childNode = new TrieNode();
                // first insert, then add the field
                // start from byteIndex with newLength.
                // newLength = lastIndex - byteIndex + 1
                childNode.setIndex(fieldNames.size(), byteIndex, lastIndex - byteIndex + 1, bytesToStoreLength);
                childNode.setIsEndOfField(true);
                fieldNames.add(fieldName);

                searchNode.putChild(b, childNode);
                return childNode.getIndex();
            }
            // if node exists, compare the remaining byte seq
            searchNode = nextNode;

            if (searchNode.getLength() > 1) {
                // compare the byte seq
                int c = 0;
                int oldByteIndex = byteIndex;

                IValueReference storedFieldName = fieldNames.get(searchNode.getIndex());
                byte[] storedFieldBytes = storedFieldName.getByteArray();
                int storedFieldStart = searchNode.getStart();
                while (c < Math.min(searchNode.getLength(), lastIndex - oldByteIndex + 1)) {
                    if (bytes[byteIndex] != storedFieldBytes[storedFieldStart + c]) {
                        break;
                    }
                    c++;
                    byteIndex++;
                }

                // from c & byteIndex, things are not matching,
                // split into two nodes,
                // one from (c, ...) -> handled below
                // other from (byteIndex, ...) -> handled in the next iteration, as byteIndex will be absent.

                // handling (c, ..)
                int leftToSplitForCurrentNode = searchNode.getLength() - c;
                if (leftToSplitForCurrentNode > 0) {
                    searchNode.split(storedFieldName, c);
                }
            } else {
                byteIndex++;
            }
        }

        // since the node is already present,
        // point it to the current fieldName, and update the start and length based on the fieldName
        // prefix would be the same
        // find absolute starting point in the current fieldName
        int diff = searchNode.getStart() - searchNode.getBytesToStoreLength();
        // since hookup happens on a new fieldName, hence start will be bytesToStoreLength
        searchNode.setIndex(fieldNames.size(), bytesToStoreLength + diff, searchNode.getLength(), bytesToStoreLength);
        searchNode.setIsEndOfField(true);
        fieldNames.add(fieldName);
        return searchNode.getIndex();
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeInt(VERSION);

        // serialize fieldNames
        out.writeInt(fieldNames.size());
        for (IValueReference fieldName : fieldNames) {
            out.writeInt(fieldName.getLength());
            out.write(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength());
        }

        rootNode.serialize(out);
    }

    public List<IValueReference> getFieldNames() {
        return fieldNames;
    }

    public IValueReference getFieldName(int fieldIndex) {
        return fieldNames.get(fieldIndex);
    }

    public void clear() {
        rootNode = null;
        fieldNames.clear();
    }

    public static FieldNameTrie deserialize(DataInput in) throws IOException {
        int version = in.readInt();
        if (version == VERSION) {
            return deserializeV1(in);
        }
        throw new IllegalStateException("Unsupported version: " + version);
    }

    private static FieldNameTrie deserializeV1(DataInput in) throws IOException {
        int numberOfFieldNames = in.readInt();

        List<IValueReference> fieldNames = new ArrayList<>();
        deserializeFieldNames(in, fieldNames, numberOfFieldNames);

        FieldNameTrie newTrie = new FieldNameTrie(fieldNames);
        newTrie.rootNode = TrieNode.deserialize(in);

        return newTrie;
    }

    @Override
    public String toString() {
        TrieNode currentNode = rootNode;
        Queue<TrieNode> queue = new ArrayDeque<>();
        currentNode.getChildren().addAllChildren(queue);
        StringBuilder treeBuilder = new StringBuilder();
        while (!queue.isEmpty()) {
            int len = queue.size();
            for (int i = 0; i < len; i++) {
                TrieNode node = queue.poll();
                assert node != null;
                byte[] bytes = fieldNames.get(node.getIndex()).getByteArray();
                for (int j = 0; j < node.getLength(); j++) {
                    treeBuilder.append((char) bytes[node.getStart() + j]);
                }
                treeBuilder.append("(").append(node.isEndOfField()).append(")");
                if (i != len - 1) {
                    treeBuilder.append(" | ");
                }

                node.getChildren().addAllChildren(queue);
            }
            treeBuilder.append("\n");
        }
        return treeBuilder.toString();
    }

    private static class LookupState {
        private TrieNode lastNode;
        private int relativeOffsetFromStart;
        private int fieldLength;

        public void setState(TrieNode lastNode, int startIndex, int continuationByteIndex, int fieldLength) {
            this.lastNode = lastNode;
            this.relativeOffsetFromStart = continuationByteIndex - startIndex;
            this.fieldLength = fieldLength;
        }

        public TrieNode getLastNode() {
            return lastNode;
        }

        public int getRelativeOffsetFromStart() {
            return relativeOffsetFromStart;
        }

        public int getFieldLength() {
            return fieldLength;
        }
    }
}
