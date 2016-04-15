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
package org.apache.hyracks.dataflow.std.parallel.histogram.terneray;

import org.apache.hyracks.dataflow.std.parallel.IStatisticTernaryNode;

/**
 * @author michael
 *         This node consists of an optional payload field for incremental sequential accessor.
 */
public class MemoryTernaryNode<T> implements IStatisticTernaryNode<T> {
    private T payload = null;
    private char key = 0;
    private IStatisticTernaryNode<T> left = null;
    private IStatisticTernaryNode<T> right = null;
    private IStatisticTernaryNode<T> middle = null;
    private int id = -1;
    private boolean grown = false;
    //The level limit 16 bits plus the current level 16 bits.
    private int level = 0;
    private int count = 0;

    public MemoryTernaryNode() {
    }

    public MemoryTernaryNode(int level) {
        this.level = level;
    }

    public MemoryTernaryNode(short limit, short level) {
        this.level |= limit << 16;
        this.level |= level << 0;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean isActive() {
        return (getLimit() > getLevel());
    }

    @Override
    public char getKey() {
        return key;
    }

    @Override
    public void setKey(char key) {
        this.key = key;
    }

    @Override
    public IStatisticTernaryNode<T> getLeft() {
        return left;
    }

    @Override
    public void setLeft(IStatisticTernaryNode<T> left) {
        this.left = left;
    }

    @Override
    public IStatisticTernaryNode<T> getRight() {
        return right;
    }

    @Override
    public void setRight(IStatisticTernaryNode<T> right) {
        this.right = right;
    }

    @Override
    public IStatisticTernaryNode<T> getMiddle() {
        return middle;
    }

    @Override
    public void setMiddle(IStatisticTernaryNode<T> middle) {
        this.middle = middle;
    }

    @Override
    public short getLimit() {
        return (short) (level >> 16);
    }

    @Override
    public void setLimit(int limit) {
        level &= 0xffff;
        level |= (limit & 0xffff) << 16;
    }

    @Override
    public short getLevel() {
        return (short) (level & 0xffff);
    }

    @Override
    public void setLevel(int limit) {
        level &= 0xffff0000;
        level |= (limit & 0xffff) << 0;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public void clearCount() {
        this.count = 0;
    }

    @Override
    public void updateBy(int ub) {
        this.count += ub;
    }

    @Override
    public void setPayload(T payload) {
        this.payload = payload;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public void setGrown() {
        this.grown = true;
    }

    @Override
    public boolean isGrown() {
        return grown;
    }
}
