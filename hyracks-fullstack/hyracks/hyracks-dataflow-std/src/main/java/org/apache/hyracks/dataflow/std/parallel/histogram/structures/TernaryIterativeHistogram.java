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

package org.apache.hyracks.dataflow.std.parallel.histogram.structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.std.parallel.IHistogram;
import org.apache.hyracks.dataflow.std.parallel.IIterativeHistogram;
import org.apache.hyracks.dataflow.std.parallel.histogram.terneray.SequentialAccessor;
import org.apache.hyracks.dataflow.std.parallel.histogram.terneray.TernaryMemoryTrie;
import org.apache.hyracks.dataflow.std.parallel.util.DualSerialEntry;
import org.apache.hyracks.dataflow.std.parallel.util.HistogramUtils;
import org.apache.hyracks.dataflow.std.parallel.util.ValueSerialEntry;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * @author michael
 *         Comments: call <initialized, needIteration, generate, getFixPointable>*
 */
public class TernaryIterativeHistogram implements IIterativeHistogram<UTF8StringPointable> {
    private static final Logger LOGGER = Logger.getLogger(TernaryIterativeHistogram.class.getName());

    private static final boolean printQuantiles = false;
    private final static int DEFAULT_INITIAL_LENGTH = 5;
    private final List<Entry<String, Integer>> stbPrefixies;
    private final List<Entry<String, Integer>> dspPrefixies;
    private final List<Entry<String, Integer>> outPrefixies;
    private final boolean fixPointable;
    private final double balanceFactor;
    private final int outputCards;

    private List<Entry<String, Integer>> fixPrefixies = null;
    private TernaryMemoryTrie<SequentialAccessor> tmt;
    private boolean iterating = false;
    private short prefixLength = 0;
    private long totalCount = 0;
    private long partialCount = 0;
    private final boolean selfGrow;

    /*private int redundant = 0;
    private int ommitednt = 0;*/

    public TernaryIterativeHistogram(int outputCards, double bf, boolean fixPointable, boolean selfGrow) {
        this.stbPrefixies = new ArrayList<Entry<String, Integer>>();
        //For the single value histogram in the future.
        this.fixPointable = fixPointable;
        this.dspPrefixies = new ArrayList<Entry<String, Integer>>();
        this.outPrefixies = new ArrayList<Entry<String, Integer>>();
        this.outputCards = outputCards;
        this.balanceFactor = bf;
        this.selfGrow = selfGrow;
        if (fixPointable)
            fixPrefixies = new ArrayList<Entry<String, Integer>>();
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#initialize()
     */
    @Override
    public void initialize() {
        if (dspPrefixies.size() > 0) {
            iterating = true;
            stbPrefixies.clear();
            prefixLength *= 2;
            partialCount = 0;
        } else {
            prefixLength = DEFAULT_INITIAL_LENGTH;
            partialCount = 0;
        }
        if (!selfGrow || tmt == null)
            tmt = new TernaryMemoryTrie<SequentialAccessor>(prefixLength, selfGrow);
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#getType()
     */
    @Override
    public FieldType getType() {
        // TODO Auto-generated method stub
        return FieldType.UTF8;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#merge(org.apache.hyracks.dataflow.std.sample.IHistogram)
     */
    @Override
    public void merge(IHistogram<UTF8StringPointable> ba) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#addItem(java.lang.Object)
     */
    @Override
    public void addItem(UTF8StringPointable item) throws HyracksDataException {
        boolean success = false;
        if (iterating) {
            if (item.getUTF8Length() > prefixLength / 2 + 1) {
                StringBuilder sb = new StringBuilder();
                item.toString(sb);
                for (int i = 0; i < dspPrefixies.size(); i++) {
                    if (dspPrefixies.get(i).getKey().equals(sb.substring(0, prefixLength / 2 + 1))) {
                        success = tmt.insert(new SequentialAccessor(sb.toString()), 0);
                        if (!success) {
                            partialCount++;
                            throw new HyracksDataException(sb.toString() + " length: " + item.getLength()
                                    + " constraint: " + prefixLength);
                        }
                        break;
                    }
                }
            }
        } else {
            totalCount++;
            StringBuilder sb = new StringBuilder();
            item.toString(sb);
            success = tmt.insert(new SequentialAccessor(sb.toString()), 0);
            if (!success)
                throw new HyracksDataException(item.getLength() + " out of: " + prefixLength);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#countItem(java.lang.Object)
     */
    @Override
    public void countItem(UTF8StringPointable item) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#countReset()
     */
    @Override
    public void countReset() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#getCurrent()
     */
    @Override
    public int getCurrent() throws HyracksDataException {
        // TODO Auto-generated method stub
        return 0;
    }

    private void orderPrefixies(List<Entry<String, Integer>> prefixies, boolean compress, boolean isGlobal)
            throws HyracksDataException {
        List<DualSerialEntry<String, Integer>> serialOutput = new ArrayList<DualSerialEntry<String, Integer>>();
        for (Entry<String, Integer> e : prefixies)
            serialOutput.add(new DualSerialEntry<String, Integer>(e.getKey(), e.getValue(), false, false));
        Collections.sort(serialOutput);
        prefixies.clear();
        if (compress)
            compress(serialOutput, prefixies, isGlobal);
        else
            for (Entry<String, Integer> e : serialOutput)
                prefixies.add(e);
    }

    //Unordered ticks for rangeMap merge
    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#generate()
     */
    @Override
    public List<Entry<UTF8StringPointable, Integer>> generate(boolean isGlobal) throws HyracksDataException {
        orderPrefixies(outPrefixies, true, isGlobal);
        return convertMergeToUTF8(outPrefixies);
    }

    private void compress(List<DualSerialEntry<String, Integer>> in, List<Entry<String, Integer>> out, boolean isGlobal)
            throws HyracksDataException {
        String quantileOut = "";
        double threshold = 0;
        int quantiles = 0;
        if (isGlobal) {
            threshold = totalCount / outputCards;
            quantiles = outputCards;
        } else {
            threshold = (totalCount * balanceFactor) / outputCards;
            quantiles = in.size();
        }
        int count = 0;
        int iPart = 0;
        int iCur = 0;
        for (int i = 0; i < in.size(); i++) {
            if (printQuantiles) {
                String sb = in.get(i).getKey();
                quantileOut += sb.toString() + " : " + in.get(i).getValue() + "\n";
            }
            if (iPart < quantiles - 1 && (iCur + in.get(i).getValue() / 2) > threshold * (iPart + 1)) {
                if (i > 0)
                    out.add(in.get(i - 1));
                else
                    out.add(in.get(i));
                out.get(out.size() - 1).setValue(count);
                count = 0;
                iPart++;
            }
            count += in.get(i).getValue();
            iCur += in.get(i).getValue();
        }
        if (isGlobal && in.size() > 0) {
            out.add(in.get(in.size() - 1));
            out.get(out.size() - 1).setValue(count);
        }
        if (printQuantiles)
            LOGGER.info(quantileOut);
        quantiles = 0;
        count = 0;
        for (int i = 0; i < out.size(); i++)
            quantiles += out.get(i).getValue();
        for (int i = 0; i < in.size(); i++)
            count += in.get(i).getValue();
        LOGGER.info("Before merge: " + in.size() + " After merge: " + out.size() + " on: " + quantiles + " out of "
                + count);
    }

    private void disperse(String path, boolean deeper) {
        tmt.grow(new SequentialAccessor(path), deeper, prefixLength);
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IIterativeHistogram#disperse(java.lang.Object, int)
     */
    @Override
    public void disperse() throws HyracksDataException {
        // TODO Auto-generated method stub
        initialize();
        for (Entry<String, Integer> entry : dspPrefixies) {
            disperse(entry.getKey(), true);
        }
        if (fixPointable) {
            for (Entry<String, Integer> entry : fixPrefixies)
                disperse(entry.getKey(), false);
        }
    }

    private Map<String, Boolean> updateIteration() throws HyracksDataException {
        stbPrefixies.clear();
        tmt.serialize(prefixLength);
        Entry<String, Integer> entry = null;
        Map<String, Boolean> genExtensible = new HashMap<String, Boolean>();
        while ((entry = tmt.next(true)) != null) {
            stbPrefixies.add(entry);
            if (entry.getKey().length() == prefixLength + 1)
                genExtensible.put(entry.getKey(), true);
            else {
                genExtensible.put(entry.getKey(), false);
            }
        }
        return genExtensible;
    }

    private void outputPrefix(Map<String, Integer> fatherMap, Entry<String, Integer> childEntry) {
        if (prefixLength == DEFAULT_INITIAL_LENGTH)
            return;
        int fatherLength = prefixLength / 2;
        try {
            String key = childEntry.getKey().substring(0, fatherLength + 1);
            fatherMap.put(key, fatherMap.get(key) - childEntry.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean needIteration() throws HyracksDataException {
        boolean ret = false;
        Map<String, Boolean> genExtensible = updateIteration();
        Map<String, Integer> oldPrefixies = new HashMap<String, Integer>();
        for (Iterator<Entry<String, Integer>> itr = dspPrefixies.iterator(); itr.hasNext();) {
            Entry<String, Integer> entry = itr.next();
            oldPrefixies.put(entry.getKey(), entry.getValue());
        }
        dspPrefixies.clear();
        if (stbPrefixies.size() <= 0) {
            for (Iterator<Entry<String, Integer>> itr = oldPrefixies.entrySet().iterator(); itr.hasNext();) {
                Entry<String, Integer> entry = itr.next();
                if (entry.getValue() > 0) {
                    disperse(entry.getKey(), false);
                    outPrefixies.add(entry);
                }
            }
            LOGGER.warning("Double calling iterations without freshing the sampling data");
            return false;
        }
        for (Entry<String, Integer> e : stbPrefixies) {
            outputPrefix(oldPrefixies, e);
            if (e.getValue() > 1 && e.getValue() > (double) totalCount / outputCards * balanceFactor) {
                if (genExtensible.get(e.getKey())) {
                    dspPrefixies.add(e);
                    ret = true;
                } else {
                    if (fixPointable)
                        fixPrefixies.add(e);
                    else {
                        disperse(e.getKey(), false);
                        outPrefixies.add(e);
                    }
                }
            } else {
                disperse(e.getKey(), false);
                outPrefixies.add(e);
            }
        }
        for (Iterator<Entry<String, Integer>> itr = oldPrefixies.entrySet().iterator(); itr.hasNext();) {
            Entry<String, Integer> entry = itr.next();
            if (entry.getValue() > 0) {
                disperse(entry.getKey(), false);
                outPrefixies.add(entry);
            }
        }
        int counto = testCount(outPrefixies);
        int countd = testCount(dspPrefixies);
        int counta = counto + countd;
        int countf = 0;
        if (fixPointable)
            countf = testCount(fixPrefixies);
        LOGGER.info("Counto: " + counto + " countd: " + countd + " counta: " + counta + " countf: " + countf
                + " partial: " + partialCount + " length: " + prefixLength + " payload: " + tmt.getPayCount()
                + " return: " + ret);
        return ret;
    }

    private int testCount(List<Entry<String, Integer>> prefs) {
        int count = 0;
        for (Entry<String, Integer> e : prefs)
            count += e.getValue();
        return count;
    }

    @Override
    public boolean isFixPointable() throws HyracksDataException {
        return fixPointable;
    }

    @SuppressWarnings("unused")
    private List<Entry<UTF8StringPointable, Integer>> convertToUTF8(List<Entry<String, Integer>> prefixies) {
        List<Entry<UTF8StringPointable, Integer>> output = new ArrayList<Entry<UTF8StringPointable, Integer>>();
        for (Entry<String, Integer> e : prefixies) {
            UTF8StringPointable ustr = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
            byte[] buf = HistogramUtils.toUTF8Byte(e.getKey()/*.toCharArray()*/, 0);
            ustr.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
            output.add(new ValueSerialEntry<UTF8StringPointable, Integer>(ustr, e.getValue()));
        }
        return output;
    }

    private List<Entry<UTF8StringPointable, Integer>> convertMergeToUTF8(List<Entry<String, Integer>> prefixies) {
        List<Entry<UTF8StringPointable, Integer>> output = new ArrayList<Entry<UTF8StringPointable, Integer>>();
        int index = 0;
        int accumCount = 0;
        String lastKey = "";
        for (Entry<String, Integer> e : prefixies) {
            if (lastKey.equals(e.getKey())) {
                accumCount += e.getValue();
                output.get(index).setValue(accumCount);
            } else {
                UTF8StringPointable ustr = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                byte[] buf = HistogramUtils.toUTF8Byte(e.getKey()/*.toCharArray()*/, 0);
                ustr.set(buf, 0, UTF8StringUtil.getUTFLength(buf, 0));
                output.add(new ValueSerialEntry<UTF8StringPointable, Integer>(ustr, e.getValue()));
                accumCount = e.getValue();
                lastKey = e.getKey();
                index++;
            }
        }
        return output;
    }

    @Override
    public List<Entry<UTF8StringPointable, Integer>> getFixPointable() throws HyracksDataException {
        orderPrefixies(fixPrefixies, false, true);
        return convertMergeToUTF8(fixPrefixies);
    }
}
