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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hyracks.dataflow.std.parallel.ISequentialAccessor;
import org.apache.hyracks.dataflow.std.parallel.ISequentialTrie;
import org.apache.hyracks.dataflow.std.parallel.IStatisticEntity;
import org.apache.hyracks.dataflow.std.parallel.IStatisticTernaryNode;
import org.apache.hyracks.dataflow.std.parallel.util.DualSerialEntry;

/**
 * @author michael
 */
public class TernaryMemoryTrie<E extends ISequentialAccessor> implements ISequentialTrie<E>, IStatisticEntity<E> {
    @SuppressWarnings("unused")
    private static final Logger LOGGER = Logger.getLogger(TernaryMemoryTrie.class.getName());
    private static final short DEFAULT_LEVEL_STEP = 1;
    private static final short DEFAULT_ROOT_LEVEL = 0;
    private List<DualSerialEntry<String, Integer>> serialRoots = null;
    private int current = -1;
    private final boolean selfGrow;
    private int payCount = 0;
    private int nodeCount = 0;
    private int nodeCreate = 0;
    private IStatisticTernaryNode<List<E>> root = null;

    public int increaseLimit(short limit, short level) {
        int newLevel = 0;
        newLevel |= (limit + DEFAULT_LEVEL_STEP) << 16;
        newLevel |= level;
        return newLevel;
    }

    public TernaryMemoryTrie() {
        this.selfGrow = false;
    }

    public TernaryMemoryTrie(short limit, boolean grow) {
        this.selfGrow = grow;
        root = new MemoryTernaryNode<List<E>>(limit, DEFAULT_ROOT_LEVEL);
    }

    private void pinLoad(IStatisticTernaryNode<List<E>> node, E p) {
        List<E> payload = null;
        if ((payload = node.getPayload()) == null)
            node.setPayload(payload = new ArrayList<E>());
        payload.add(p);
        payCount++;
    }

    private boolean insert(E p, int id, IStatisticTernaryNode<List<E>> node) {
        boolean ret = false;
        char key;
        if (node.equals(root))
            key = p.first();
        else
            key = p.current();
        while (p.cursor() < p.length()) {
            char nodeKey = node.getKey();
            if (nodeKey == 0) {
                ret = true;
                node.setKey(key);
            }
            if (key < node.getKey()) {
                IStatisticTernaryNode<List<E>> left = node.getLeft();
                if (left == null) {
                    left = new MemoryTernaryNode<List<E>>(node.getLimit(), node.getLevel());
                    nodeCreate++;
                    node.setLeft(left);
                }
                node = left;
            } else if (key > node.getKey()) {
                IStatisticTernaryNode<List<E>> right = node.getRight();
                if (right == null) {
                    right = new MemoryTernaryNode<List<E>>(node.getLimit(), node.getLevel());
                    nodeCreate++;
                    node.setRight(right);
                }
                node = right;
            } else {
                node.updateBy(1);
                if (p.cursor() + 1 == p.length()) {
                    node.setId(id);
                    ret = true;
                } else {
                    IStatisticTernaryNode<List<E>> mid = node.getMiddle();
                    if (null == mid) {
                        if (node.isActive()) {
                            mid = new MemoryTernaryNode<List<E>>(node.getLimit(), (short) (node.getLevel() + 1));
                            nodeCreate++;
                            node.setMiddle(mid);
                            if (p.length() == p.cursor() + 1) {
                                mid.setId(id);
                                mid.updateBy(1);
                                ret = true;
                            }
                        } else {
                            node.setId(id);
                            ret = true;
                            if (selfGrow && p.cursor() == node.getLimit())
                                pinLoad(node, p);
                            break;
                        }
                    }
                    node = mid;
                }
                key = p.next();
            }
        }
        if (selfGrow && p.length() == node.getLimit() + 1 && p.current() == p.length())
            pinLoad(node, p);
        return ret;

    }

    private IStatisticTernaryNode<List<E>> traverse(IStatisticTernaryNode<List<E>> from, E p) {
        IStatisticTernaryNode<List<E>> node = null;
        int pos = 0;
        while (from != null && pos < p.length()) {
            if (p.at(pos) < from.getKey())
                from = from.getLeft();
            else if (p.at(pos) > from.getKey())
                from = from.getRight();
            else {
                if (from.getId() != -1) {
                    node = from;
                }
                from = from.getMiddle();
                pos++;
            }
        }
        return node;
    }

    //Return: true reaches the leaf, false for middle node.
    @Override
    public boolean insert(E p, int id) {
        if (null == root)
            root = new MemoryTernaryNode<List<E>>(DEFAULT_LEVEL_STEP, DEFAULT_ROOT_LEVEL);
        return insert(p, id, root);
    }

    @Override
    public void grow(E p, boolean deeper, short limit) {
        if (!selfGrow)
            return;
        IStatisticTernaryNode<List<E>> node = traverse(root, p);
        if (!deeper) {
            List<E> payload = node.getPayload();
            if (payload != null) {
                payload.clear();
                node.setPayload(null);
                payCount--;
            }
        } else {
            node.setLimit(limit);
            node.setGrown();
            Iterator<E> payload = node.getPayload().iterator();
            while (payload.hasNext()) {
                insert(payload.next(), 0, node);
            }
            node.getPayload().clear();
            node.setPayload(null);
            payCount--;
        }
    }

    @Override
    public int search(E p) {
        if (p.length() < 0)
            return -1;
        IStatisticTernaryNode<List<E>> node = traverse(root, p);
        if (node == null)
            return -1;
        else
            return node.getId();
    }

    @Override
    public boolean delete(E p) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getCount(E p) {
        if (p.length() < 0)
            return -1;
        IStatisticTernaryNode<List<E>> node = traverse(root, p);
        if (node == null)
            return -1;
        else
            return node.getCount();
    }

    @Override
    public void clearCount(E p) {
        if (p.length() < 0)
            return;
        IStatisticTernaryNode<List<E>> node = traverse(root, p);
        if (node != null)
            clearChildren(node);
    }

    private void clearChildren(IStatisticTernaryNode<List<E>> node) {
        if (node != null) {
            node.updateBy(-node.getCount());
            clearChildren(node.getLeft());
            clearChildren(node.getMiddle());
            clearChildren(node.getRight());
        }
    }

    @Override
    public void updateBy(E p, int ub) {
        if (p.length() < 0)
            return;
        IStatisticTernaryNode<List<E>> node = traverse(root, p);
        if (node != null)
            node.updateBy(ub);
    }

    private int generateSequences(IStatisticTernaryNode<List<E>> node, String path, short limit) {
        if (node == null)
            return 0;
        int leftCount = generateSequences(node.getLeft(), path, limit);
        String newPath = new String(path);
        int childCount = generateSequences(node.getMiddle(), newPath += node.getKey(), limit);
        int rightCount = generateSequences(node.getRight(), path, limit);
        if (node.getId() != -1) {
            short nodeLimit = node.getLimit();
            if (!selfGrow)
                serialRoots.add(new DualSerialEntry<String, Integer>(path + node.getKey(),
                        node.getCount() - childCount, false));
            else if (nodeLimit == limit && !node.isGrown())
                serialRoots.add(new DualSerialEntry<String, Integer>(path + node.getKey(),
                        node.getCount() - childCount, false));
            return (node.getCount() + leftCount + rightCount);
        } else {
            if (node.getMiddle() == null)
                return (node.getCount());
            else
                return (node.getCount() + rightCount + leftCount);
        }
    }

    @Override
    public void serialize(short limit) {
        if (serialRoots == null)
            serialRoots = new ArrayList<DualSerialEntry<String, Integer>>();
        else
            serialRoots.clear();
        String sroot = "";
        generateSequences(root, sroot, limit);
        Collections.sort(serialRoots);
        current = 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E next() {
        if (current < serialRoots.size()) {
            DualSerialEntry<String, Integer> se = serialRoots.get(current++);
            return (E) new SequentialAccessor(se.getKey());
        } else
            return null;
    }

    public Entry<String, Integer> next(boolean dbg) {
        if (current < serialRoots.size()) {
            DualSerialEntry<String, Integer> se = serialRoots.get(current++);
            return se;
        } else
            return null;
    }

    public int getTotal() {
        int count = 0;
        for (Entry<String, Integer> e : serialRoots) {
            count += e.getValue();
        }
        return count;
    }

    public int getPayCount() {
        return payCount;
    }

    private void iterativeClean(IStatisticTernaryNode<List<E>> node) {
        if (node == null)
            return;
        iterativeClean(node.getLeft());
        iterativeClean(node.getMiddle());
        iterativeClean(node.getRight());
        nodeCount++;
        if (node.getPayload() != null)
            payCount++;
    }

    public int verifyClean() {
        payCount = 0;
        nodeCount = 0;
        iterativeClean(root);
        return payCount;
    }

    public int nodeCount() {
        return nodeCount;
    }

    public int nodeCreate() {
        return nodeCreate;
    }
}
