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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.logging.Logger;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.dataflow.std.parallel.IDTHistogram;
import org.apache.hyracks.dataflow.std.parallel.IHistogram;
import org.apache.hyracks.dataflow.std.parallel.util.HistogramUtils;

/**
 * @author michael
 */
public class DTStreamingHistogram<E extends AbstractPointable> implements IDTHistogram<E> {
    private static final Logger LOGGER = Logger.getLogger(DTStreamingHistogram.class.getName());

    private boolean heapIncrement = false;

    private/*static final*/boolean equalHeight = false;

    private/*static final*/int QUANTILE_SCALE = 8;

    private final boolean generateWithoutReduce = false;

    private boolean adjustedBound = false;

    private final BOUNDARY_TYPE boundary = BOUNDARY_TYPE.ACCUMULATED;

    //private static final boolean streamMerge = false;

    private static final int DEFAULT_ANSI_SAMPLE_LENGTH = 9;

    private double leftmostItem;

    private double rightmostItem;

    private final boolean minMaxReproduction = true;

    private enum BOUNDARY_TYPE {
        MEDIATE,
        INTERPOLATE,
        ACCUMULATED,
        RAW
    }

    private class Coord implements Comparable<Object> {
        double x;
        int y;

        @SuppressWarnings("unchecked")
        public int compareTo(Object other) {
            return Double.compare(x, ((Coord) other).x);
        }
    };

    private double pointableToQuantile(E item) throws HyracksDataException {
        switch (type) {
            case SHORT:
                return HistogramUtils.shortMappingToQuantile(item);
            case INT:
                return HistogramUtils.integerMappingToQuantile(item);
            case LONG:
                return HistogramUtils.longMappingToQuantile(item);
            case FLOAT:
                return HistogramUtils.floatMappingToQuantile(item);
            case DOUBLE:
                return HistogramUtils.doubleMappingToQuantile(item);
            case UTF8:
                return HistogramUtils.ansiMappingToQuantile(item, 0, DEFAULT_ANSI_SAMPLE_LENGTH);
            default:
                throw new HyracksDataException("Type " + item.getClass() + " cannot be supported.");
        }
    }

    @SuppressWarnings("unchecked")
    private E quantileToPointable(double d) throws HyracksDataException {
        switch (type) {
            case SHORT:
                return (E) HistogramUtils.quantileRevertToShort(d);
            case INT:
                return (E) HistogramUtils.quantileRevertToInteger(d);
            case LONG:
                return (E) HistogramUtils.quantileRevertToLong(d);
            case FLOAT:
                return (E) HistogramUtils.quantileRevertToFloat(d);
            case DOUBLE:
                return (E) HistogramUtils.quantileRevertToDouble(d);
            case UTF8:
                return (E) HistogramUtils.quantileRevertToAnsi(d, DEFAULT_ANSI_SAMPLE_LENGTH);
            default:
                throw new HyracksDataException("Type enum " + type + " cannot be supported.");
        }
    }

    private int current;
    private int nbins;
    private int nusedbins;
    private List<Coord> bins;
    private Random prng;
    private FieldType type;
    /*private BoundedPriorityQueue<Number> domQuantiles = null;*/
    public DominantQuantile<Number> peakTest = new DominantQuantile<Number>(0, 0);
    private DominantQuantile<Number> heapHead;
    public long updateHeap = 0;

    public class DominantQuantile<T extends Number> implements Comparable<DominantQuantile<T>> {
        T dominant;
        int iBin;

        public DominantQuantile(T dom, int iBin) {
            this.dominant = dom;
            this.iBin = iBin;
        }

        public void setBin(int bin) {
            this.iBin = bin;
        }

        public void setDome(T dom) {
            this.dominant = dom;
        }

        public void update(T dom, int bin) {
            this.iBin = bin;
            this.dominant = dom;
        }

        public int getBin() {
            return iBin;
        }

        public T getDom() {
            return dominant;
        }

        @Override
        public int compareTo(DominantQuantile<T> o) {
            // TODO Auto-generated method stub
            return Double.compare(dominant.doubleValue(), o.getDom().doubleValue());
        }
    }

    public class BoundedPriorityQueue<T extends Number> extends PriorityQueue<DominantQuantile<T>> {
        private static final long serialVersionUID = 1L;
        private int limit;
        private DominantQuantile<T> peek;
        private boolean asc = true;

        public BoundedPriorityQueue(int maxCapacity, boolean asc) {
            this.limit = maxCapacity;
            this.asc = asc;
        }

        //@SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean add(DominantQuantile<T> e) {
            if (super.size() < limit) {
                boolean ret = super.add(e);
                peek = super.peek();
                return ret;
            } else {
                if (asc) {
                    if (peek.compareTo(e) < 0) {
                        super.remove();
                        boolean ret = super.add(e);
                        peek = super.peek();
                        return ret;
                    }
                } else {
                    if (peek.compareTo(e) > 0) {
                        super.remove();
                        boolean ret = super.add(e);
                        peek = super.peek();
                        return ret;
                    }
                }
            }
            return false;
        }

        public int getLimit() {
            return limit;
        }
    }

    public DTStreamingHistogram() {
        nbins = 0;
        nusedbins = 0;
        bins = null;
        prng = new Random(31183);
    }

    public DTStreamingHistogram(FieldType t) {
        this();
        type = t;
    }

    public DTStreamingHistogram(FieldType t, boolean heapActive) {
        this();
        this.type = t;
        this.heapIncrement = heapActive;
    }

    public boolean isReady() {
        return (getCurrent() != 0);
    }

    public int getNBins() {
        return nbins;
    }

    public int getNUsedBins() {
        return nusedbins;
    }

    public List<Coord> getBins() {
        return bins;
    }

    public Coord getBin(int b) {
        return bins.get(b);
    }

    public void allocate(int num_bins) {
        nbins = num_bins * QUANTILE_SCALE;
        if (heapIncrement)
            bins = new ArrayList<Coord>();
        else
            bins = new ArrayList<Coord>();
        nusedbins = 0;
        leftmostItem = Double.MAX_VALUE;
        rightmostItem = Double.MIN_VALUE;
    }

    public void allocate(int num_bins, int scale, boolean equalHeight) {
        this.QUANTILE_SCALE = scale;
        this.equalHeight = equalHeight;
        if (heapIncrement) {
            if (equalHeight) {
                //this.domQuantiles = new BoundedPriorityQueue<Number>(1, true);
                heapHead = new DominantQuantile<Number>(Integer.MAX_VALUE, -1);
            } else {
                //this.domQuantiles = new BoundedPriorityQueue<Number>(1, false);
                heapHead = new DominantQuantile<Number>(Double.MAX_VALUE, -1);
            }
        }
        allocate(num_bins);
    }

    public FieldType getType() {
        return type;
    }

    public E quantile(double q) throws HyracksDataException {
        assert (bins != null && nusedbins > 0 && nbins > 0);
        double sum = 0, csum = 0;
        int b;
        for (b = 0; b < nusedbins; b++) {
            sum += bins.get(b).y;
        }
        for (b = 0; b < nusedbins; b++) {
            csum += bins.get(b).y;
            if (csum / sum >= q) {
                if (b == 0) {
                    E ret = quantileToPointable(bins.get(b).x);
                    return ret;
                }

                csum -= bins.get(b).y;
                double r = bins.get(b - 1).x + (q * sum - csum) * (bins.get(b).x - bins.get(b - 1).x) / (bins.get(b).y);

                E ret = quantileToPointable(r);
                return ret;
            }
        }
        return null;
    }

    public void trim() {
        if (equalHeight)
            trimForHeight();
        else
            trimForWidth();
    }

    public void trimForWidth() {
        while (nusedbins > nbins) {
            double smallestdiff = bins.get(1).x - bins.get(0).x;
            int smallestdiffloc = 0;
            int smallestdiffcount = 1;
            for (int i = 1; i < nusedbins - 1; i++) {
                double diff = bins.get(i + 1).x - bins.get(i).x;
                if (diff < smallestdiff) {
                    smallestdiff = diff;
                    smallestdiffloc = i;
                    smallestdiffcount = 1;
                } else {
                    if (diff == smallestdiff && prng.nextDouble() <= (1.0 / ++smallestdiffcount)) {
                        smallestdiffloc = i;
                    }
                }
            }
            int d = bins.get(smallestdiffloc).y + bins.get(smallestdiffloc + 1).y;
            Coord smallestdiffbin = bins.get(smallestdiffloc);
            smallestdiffbin.x *= (double) smallestdiffbin.y / d;
            smallestdiffbin.x += bins.get(smallestdiffloc + 1).x / d * bins.get(smallestdiffloc + 1).y;
            smallestdiffbin.y = d;
            peakTest.update(smallestdiff, smallestdiffloc);
            bins.remove(smallestdiffloc + 1);
            nusedbins--;
            updateHeap++;
        }
    }

    public void trimForHeight() {
        while (nusedbins > nbins) {
            int maxHeightSum = bins.get(1).y + bins.get(0).y;
            int maxHeightLoc = 0;
            int smallestdiffcount = 1;
            for (int i = 1; i < nusedbins - 1; i++) {
                int curHeightSum = bins.get(i + 1).y + bins.get(i).y;
                if (curHeightSum < maxHeightSum) {
                    maxHeightSum = curHeightSum;
                    maxHeightLoc = i;
                    smallestdiffcount = 1;
                } else {
                    if (curHeightSum == maxHeightSum && prng.nextDouble() <= (1.0 / ++smallestdiffcount)) {
                        maxHeightLoc = i;
                    }
                }
            }
            int d = bins.get(maxHeightLoc).y + bins.get(maxHeightLoc + 1).y;
            Coord smallestdiffbin = bins.get(maxHeightLoc);
            smallestdiffbin.x *= (double) smallestdiffbin.y / d;
            smallestdiffbin.x += bins.get(maxHeightLoc + 1).x / d * bins.get(maxHeightLoc + 1).y;
            smallestdiffbin.y = d;
            peakTest.update(maxHeightSum, maxHeightLoc);
            bins.remove(maxHeightLoc + 1);
            nusedbins--;
            updateHeap++;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#initialize()
     */
    @Override
    public void initialize() {
        // TODO Auto-generated method stub
        bins = null;
        nbins = nusedbins = 0;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#merge(org.apache.hyracks.dataflow.std.sample.IHistogram)
     */
    @Override
    public void merge(IHistogram<E> ba) throws HyracksDataException {
        if (null == ba) {
            return;
        }

        if (!(ba instanceof DTStreamingHistogram)) {
            LOGGER.info("Sampling error: " + ba.getCurrent());
            throw new HyracksDataException("Failed to get the proper sampling bins.");
        } else if (type != ba.getType())
            throw new HyracksDataException("Mismatching hitogram type.");

        DTStreamingHistogram<E> other = (DTStreamingHistogram<E>) ba;
        if (nbins == 0 || nusedbins == 0) {
            nbins = other.getNBins();
            nusedbins = other.getNUsedBins();
            bins = new ArrayList<Coord>(nusedbins);
            for (int i = 0; i < nusedbins; i++) {
                Coord bin = new Coord();
                bin.x = other.getBin(i).x;
                bin.y = other.getBin(i).y;
                bins.add(bin);
            }
        } else {
            List<Coord> tmpbins = new ArrayList<Coord>(nusedbins + other.getNUsedBins());
            for (int i = 0; i < nusedbins; i++) {
                Coord bin = new Coord();
                bin.x = bins.get(i).x;
                bin.y = bins.get(i).y;
                tmpbins.add(bin);
            }
            for (int i = 0; i < other.getBins().size(); i++) {
                Coord bin = new Coord();
                bin.x = other.getBin(i).x;
                bin.y = other.getBin(i).y;
                bins.add(bin);
            }
            Collections.sort(tmpbins);
            bins = tmpbins;
            nusedbins += other.getNBins();
            trim();
        }
    }

    private double quantileMerge(double q1, double q2, int k1, int k2) {
        double q = .0;
        q = (q1 * k1 + q2 * k2) / (k1 + k2);
        return q;
    }

    private void selectedBinUpdateByOne(int requirePoint, double q) throws HyracksDataException {
        int d = bins.get(requirePoint).y + 1;
        Coord selectedMergeBin = bins.get(requirePoint);
        selectedMergeBin.x *= (double) selectedMergeBin.y / d;
        selectedMergeBin.x += q / d;
        selectedMergeBin.y = d;
    }

    private void selectedBinMergeWithNext(int mergingPoint) throws HyracksDataException {
        Coord mergeLeft = bins.get(mergingPoint);
        int d = bins.get(mergingPoint).y + bins.get(mergingPoint + 1).y;
        mergeLeft.x *= (double) mergeLeft.y / d;
        mergeLeft.x += bins.get(mergingPoint + 1).x / d * bins.get(mergingPoint + 1).y;
        mergeLeft.y = d;
        bins.remove(mergingPoint + 1);
    }

    private void atomicInsert(int bin, double q) throws HyracksDataException {
        if (equalHeight) {
            int requirePoint = -1;
            if (bin > 0 && bin <= nusedbins - 1) {
                requirePoint = (bins.get(bin - 1).y > bins.get(bin).y) ? bin : (bin - 1);
            } else if (bin == 0) {
                requirePoint = 0;
            } else if (bin == nusedbins) {
                requirePoint = nusedbins - 1;
            } else {
                throw new HyracksDataException("Invalid required position for minSum: " + bin + " out of " + nusedbins);
            }
            if (requirePoint == heapHead.getBin() || requirePoint - 1 == heapHead.getBin()) {
                //before: [a1], [1], [a2]; 
                //after: case1:[a1 + 1], [a2]; case2: [a1], [a2 + 1]; both violate the peak limit of heap;
                selectedBinUpdateByOne(requirePoint, q);
                sequentialScanAndUpdatePeak();
            } else if (heapHead.getDom().intValue() >= bins.get(requirePoint).y + 1) {
                //the most common case: merge 1 with requirePoint and keep heap unchanged.
                selectedBinUpdateByOne(requirePoint, q);
            } else {
                //merge the heap point and insert the [q, 1] into the bins.
                Coord newBin = new Coord();
                newBin.x = q;
                newBin.y = 1;
                bins.add(bin, newBin);
                if (heapHead.getBin() >= bin)
                    heapHead.setBin(heapHead.getBin() + 1);
                selectedBinMergeWithNext(heapHead.getBin());
                sequentialScanAndUpdatePeak();
            }
        } else {
            int requirePoint = -1;
            double expectedMinDiff = .0;
            boolean expectedLeftMerge = true;
            if (bin > 0 && bin <= nusedbins - 1) {
                if (q - bins.get(bin - 1).x > bins.get(bin).x - q) {
                    expectedLeftMerge = false;
                    expectedMinDiff = quantileMerge(q, bins.get(bin).x, 1, bins.get(bin).y) - bins.get(bin - 1).x;
                    requirePoint = bin;
                } else {
                    expectedLeftMerge = true;
                    expectedMinDiff = bins.get(bin).x - quantileMerge(bins.get(bin - 1).x, q, bins.get(bin - 1).y, 1);
                    requirePoint = bin - 1;
                }
            } else if (bin == 0) {
                expectedLeftMerge = false;
                expectedMinDiff = bins.get(1).x - quantileMerge(q, bins.get(0).x, 1, bins.get(0).y);
                requirePoint = 0;
            } else if (bin == nusedbins) {
                expectedLeftMerge = true;
                expectedMinDiff = quantileMerge(bins.get(nusedbins - 1).x, q, bins.get(nusedbins - 1).y, 1)
                        - bins.get(nusedbins - 1).x;
                requirePoint = nusedbins - 1;
            } else
                throw new HyracksDataException("Invalid required position for minDiff: " + bin + " out of " + nusedbins);
            if (!expectedLeftMerge && requirePoint == heapHead.getBin() || expectedLeftMerge
                    && requirePoint - 1 == heapHead.getBin()) {
                selectedBinUpdateByOne(requirePoint, q);
                sequentialScanAndUpdatePeak();
            } else if (heapHead.getDom().doubleValue() >= expectedMinDiff) {
                selectedBinUpdateByOne(requirePoint, q);
                if (requirePoint != 0 && requirePoint != nusedbins - 1)
                    heapHead.update(expectedMinDiff, requirePoint);
            } else {
                Coord newBin = new Coord();
                newBin.x = q;
                newBin.y = 1;
                bins.add(bin, newBin);
                if (heapHead.getBin() >= bin)
                    heapHead.setBin(heapHead.getBin() + 1);
                selectedBinMergeWithNext(heapHead.getBin());
                sequentialScanAndUpdatePeak();
            }
        }
    }

    private void sequentialScanAndUpdatePeak() throws HyracksDataException {
        updateHeap++;
        if (equalHeight) {
            int minHeightSum = bins.get(1).y + bins.get(0).y;
            int minHeightLoc = 0;
            int smallestdiffcount = 1;
            for (int i = 1; i < nusedbins - 1; i++) {
                int curHeightSum = bins.get(i + 1).y + bins.get(i).y;
                if (curHeightSum < minHeightSum) {
                    minHeightSum = curHeightSum;
                    minHeightLoc = i;
                    smallestdiffcount = 1;
                } else {
                    if (curHeightSum == minHeightSum && prng.nextDouble() <= (1.0 / ++smallestdiffcount)) {
                        minHeightLoc = i;
                    }
                }
            }
            heapHead.update(minHeightSum, minHeightLoc);
        } else {
            double minDiffQut = bins.get(1).x - bins.get(0).x;
            int minDiffLoc = 0;
            int smallestdiffcount = 1;
            for (int i = 1; i < nusedbins - 1; i++) {
                double curDiffQut = bins.get(i + 1).x - bins.get(i).x;
                if (curDiffQut < minDiffQut) {
                    minDiffQut = curDiffQut;
                    minDiffLoc = i;
                    smallestdiffcount = 1;
                } else {
                    if (curDiffQut == minDiffQut && prng.nextDouble() <= (1.0 / ++smallestdiffcount)) {
                        minDiffLoc = i;
                    }
                }
            }
            heapHead.update(minDiffQut, minDiffLoc);
        }
    }

    private void updateHeap(int requirePoint) {
        if (equalHeight) {
            if (requirePoint > 0 && requirePoint < nusedbins - 1) {
                Integer dom = bins.get(requirePoint - 1).y + bins.get(requirePoint).y;
                if (heapHead.getDom().intValue() > dom)
                    heapHead.update(dom, requirePoint - 1);
                dom = bins.get(requirePoint).y + bins.get(requirePoint + 1).y;
                if (heapHead.getDom().intValue() > dom)
                    heapHead.update(dom, requirePoint);
            } else if (requirePoint == 0) {
                Integer dom = bins.get(0).y + bins.get(1).y;
                if (heapHead.getDom().intValue() > dom)
                    heapHead.update(dom, 0);
            } else if (requirePoint == nusedbins - 1) {
                Integer dom = bins.get(nusedbins - 2).y + bins.get(nusedbins - 1).y;
                if (heapHead.getDom().intValue() > dom)
                    heapHead.update(dom, nusedbins - 2);
            }
        } else {
            if (requirePoint > 0 && requirePoint < nusedbins - 1) {
                Double dom = bins.get(requirePoint).y - bins.get(requirePoint - 1).x;
                if (heapHead.getDom().doubleValue() > dom)
                    heapHead.update(dom, requirePoint - 1);
                dom = bins.get(requirePoint + 1).x - bins.get(requirePoint).x;
                if (heapHead.getDom().doubleValue() > dom)
                    heapHead.update(dom, requirePoint);
            } else if (requirePoint == 0) {
                Double dom = bins.get(1).x - bins.get(0).x;
                if (heapHead.getDom().doubleValue() > dom)
                    heapHead.update(dom, 0);
            } else if (requirePoint == nusedbins - 1) {
                Double dom = bins.get(nusedbins - 1).x - bins.get(nusedbins - 2).x;
                if (heapHead.getDom().doubleValue() > dom)
                    heapHead.update(dom, nusedbins - 2);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#addItem(byte[])
     */
    @Override
    public void addItem(E item) throws HyracksDataException {
        double q = pointableToQuantile(item);
        if (q < leftmostItem)
            leftmostItem = q;
        if (q > rightmostItem)
            rightmostItem = q;
        int bin = 0;
        for (int l = 0, r = nusedbins; l < r;) {
            bin = (l + r) / 2;
            if (bins.get(bin).x > q) {
                r = bin;
            } else {
                if (bins.get(bin).x < q) {
                    l = ++bin;
                } else {
                    break;
                }
            }
        }
        if (bin < nusedbins && bins.get(bin).x == q) {
            bins.get(bin).y++;
        } else {
            if (heapIncrement) {
                if (nusedbins < nbins) {
                    Coord newBin = new Coord();
                    newBin.x = q;
                    newBin.y = 1;
                    bins.add(bin, newBin);
                    nusedbins++;
                    if (nusedbins > 1) {
                        if (heapHead.getBin() >= bin)
                            heapHead.setBin(heapHead.getBin() + 1);
                        updateHeap(bin);
                    }
                } else {
                    atomicInsert(bin, q);
                }
            } else {
                Coord newBin = new Coord();
                newBin.x = q;
                newBin.y = 1;
                bins.add(bin, newBin);
                if (++nusedbins > nbins) {
                    trim();
                }
            }
        }
    }

    public class Quantile<K, V> implements Entry<K, V> {
        private K key;
        private V value;

        public Quantile(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V old = this.value;
            this.value = value;
            return old;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#getCurrent()
     */
    @Override
    public int getCurrent() {
        // TODO Auto-generated method stub
        return current;
    }

    private double[] leftExtend(double leftX, double leftY, double rightX, double rightY) {
        double pointX = 0;
        double pointY = 0;
        if (minMaxReproduction) {
            pointY = 1;
            pointX = leftmostItem;
        } else {
            pointY = 2 * leftY - rightY;
            pointX = 2 * leftX - rightX;
        }
        double point[] = new double[2];
        point[0] = pointX;
        if (minMaxReproduction)
            point[1] = pointY;
        else
            point[1] = 0;
        return point;
    }

    private double[] rightExtend(double leftX, double leftY, double rightX, double rightY) {
        double pointY = 0;
        double pointX = 0;
        if (minMaxReproduction) {
            pointY = 1;
            pointX = rightmostItem;
        } else {
            pointY = 2 * rightY - leftY;
            pointX = 2 * rightX - leftX;
        }
        double point[] = new double[2];
        point[0] = pointX;
        if (minMaxReproduction)
            point[1] = pointY;
        else
            point[1] = 0;
        return point;
    }

    private double[] accumulate(double want, double leftX, double leftY, double rightX, double rightY, double localX,
            double elipsed) {
        double localY = leftY + (rightY - leftY) * (localX - leftX) / (rightX - leftX);
        double pointY = Math.sqrt(localY * localY + 2 * want * (rightY - leftY));
        double pointX = localX + 2 * (rightX - leftX) / (pointY + localY) * want;
        double point[] = new double[2];
        point[0] = pointX;
        point[1] = /*pointY*/want;
        return point;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.sample.IHistogram#generate()
     */
    @Override
    public List<Entry<E, Integer>> generate(boolean isGlobal) throws HyracksDataException {
        List<Entry<E, Integer>> ret = new ArrayList<Entry<E, Integer>>();
        if (adjustedBound) {
            for (int i = 0; i < nusedbins; i++) {
                E pQuan = quantileToPointable(bins.get(i).x);
                ret.add(new Quantile<E, Integer>(pQuan, bins.get(i).y));
            }
        } else {
            switch (boundary) {
                case MEDIATE: {
                    for (int i = 0; i < nusedbins - 1; i++) {
                        E pQuan = mediate(quantileToPointable(bins.get(i).x), quantileToPointable(bins.get(i + 1).x));
                        ret.add(new Quantile<E, Integer>(pQuan, bins.get(i).y));
                    }
                    ret.add(new Quantile<E, Integer>(quantileToPointable(Double.MAX_VALUE), bins.get(nbins - 1).y));
                    break;
                }
                case INTERPOLATE: {
                    if (generateWithoutReduce) {
                        for (int i = 0; i < nusedbins - 1; i++) {
                            E pQuan = mediate(quantileToPointable(bins.get(i).x),
                                    quantileToPointable(bins.get(i + 1).x));
                            ret.add(new Quantile<E, Integer>(pQuan, bins.get(i).y));
                        }
                        ret.add(new Quantile<E, Integer>(quantileToPointable(Double.MAX_VALUE), bins.get(nbins - 1).y));
                        break;
                    } else {
                        int count = 0;
                        for (int i = 0; i < nusedbins - 1; i++) {
                            count += bins.get(i).y;
                            if ((i + 1) % QUANTILE_SCALE == 0) {
                                Coord cod = interpolate(bins.get(i), bins.get(i + 1));
                                ret.add(new Quantile<E, Integer>(quantileToPointable(cod.x), count));
                                count = 0;
                            }
                        }
                        ret.add(new Quantile<E, Integer>(quantileToPointable(Double.MAX_VALUE), count
                                + bins.get(nbins - 1).y));
                        break;
                    }
                }
                case ACCUMULATED: {
                    List<Coord> cacheBins = new ArrayList<Coord>();
                    cacheBins.addAll(bins);
                    long total = 0;
                    for (int i = 0; i < nusedbins; i++)
                        total += cacheBins.get(i).y;
                    double[] leftVirtual = leftExtend(cacheBins.get(0).x, cacheBins.get(0).y, cacheBins.get(1).x,
                            cacheBins.get(1).y);
                    double[] rightVirtual = rightExtend(cacheBins.get(nusedbins - 2).x, cacheBins.get(nusedbins - 2).y,
                            cacheBins.get(nusedbins - 1).x, cacheBins.get(nusedbins - 1).y);
                    Coord leftExt = new Coord();
                    leftExt.x = leftVirtual[0];
                    leftExt.y = (int) leftVirtual[1];
                    cacheBins.add(0, leftExt);
                    Coord rightExt = new Coord();
                    rightExt.x = rightVirtual[0];
                    rightExt.y = (int) rightVirtual[1];
                    cacheBins.add(rightExt);

                    int nParts = nusedbins / QUANTILE_SCALE;
                    double expection = (double) total / nParts;
                    double accd = .0;
                    int current = 0;
                    double localX = bins.get(0).x;
                    double elipsed = .0;
                    for (int i = 0; i < nParts - 1; i++) {
                        Coord cur = new Coord();
                        while (true) {
                            if (current == cacheBins.size() - 1)
                                break;
                            if ((double) (cacheBins.get(current).y + cacheBins.get(current + 1).y) / 2 - elipsed > expection
                                    - accd) {
                                double[] quan = accumulate(expection - accd, cacheBins.get(current).x,
                                        cacheBins.get(current).y, cacheBins.get(current + 1).x,
                                        cacheBins.get(current + 1).y, localX, elipsed);
                                cur.x = quan[0];
                                cur.y = (int) expection;
                                localX = quan[0];
                                elipsed += quan[1];
                                ret.add(new Quantile<E, Integer>(quantileToPointable(cur.x), cur.y));
                                accd = 0;
                                break;
                            } else if ((double) (cacheBins.get(current).y + cacheBins.get(current + 1).y) / 2 - elipsed == expection
                                    - accd) {
                                ret.add(new Quantile<E, Integer>(quantileToPointable(cacheBins.get(current + 1).x),
                                        (int) expection));
                                current++;
                                localX = bins.get(current).x;
                                elipsed = .0;
                                accd = 0;
                                break;
                            } else {
                                accd += (double) (cacheBins.get(current).y + cacheBins.get(current + 1).y) / 2
                                        - elipsed;
                                current++;
                                localX = bins.get(current).x;
                                elipsed = .0;
                            }
                        }
                    }
                    ret.add(new Quantile<E, Integer>(quantileToPointable(Double.MAX_VALUE), (int) expection));
                    break;
                }
                case RAW: {
                    for (int i = 0; i < nusedbins; i++) {
                        E pQuan = quantileToPointable(bins.get(i).x);
                        ret.add(new Quantile<E, Integer>(pQuan, bins.get(i).y));
                    }
                    break;
                }
            }
        }
        return ret;
    }

    @Override
    public void countItem(E item) throws HyracksDataException {
        double q = pointableToQuantile(item);
        int bin = 0;
        for (int l = 0, r = nusedbins; l < r;) {
            bin = (l + r) / 2;
            if (bins.get(bin).x > q) {
                r = bin;
            } else {
                if (bins.get(bin).x < q) {
                    l = ++bin;
                } else {
                    break;
                }
            }
        }
        int mark = 0;
        if (bin == nusedbins) {
            mark = bin - 1;
        } else {
            mark = bin;
        }
        bins.get(mark).y += 1;
    }

    @Override
    public void countReset() throws HyracksDataException {
        if (!adjustedBound) {
            switch (boundary) {
                case MEDIATE: {
                    for (int i = 0; i < nusedbins - 1; i++) {
                        bins.get(i).x = (bins.get(i).x + bins.get(i + 1).x) / 2;
                    }
                    bins.get(bins.size() - 1).x = Double.MAX_VALUE;
                    break;
                }
                case INTERPOLATE: {
                    for (int i = 0; i < nusedbins - 1; i++) {
                        Coord ret = interpolate(bins.get(i), bins.get(i + 1));
                        bins.get(i).x = ret.x;
                        bins.get(i).y = ret.y;
                    }
                    bins.get(bins.size() - 1).x = Double.MAX_VALUE;
                    Iterator<Coord> iter = bins.iterator();
                    int i = 0;
                    while (iter.hasNext()) {
                        iter.next();
                        if ((i++ + 1) % QUANTILE_SCALE != 0)
                            iter.remove();
                    }
                    nusedbins = bins.size();
                    nbins = bins.size();
                    break;
                }
                case ACCUMULATED: {
                    long total = 0;
                    for (int i = 0; i < nusedbins; i++)
                        total += bins.get(i).y;
                    double[] leftVirtual = leftExtend(bins.get(0).x, bins.get(0).y, bins.get(1).x, bins.get(1).y);
                    double[] rightVirtual = rightExtend(bins.get(nusedbins - 2).x, bins.get(nusedbins - 2).y,
                            bins.get(nusedbins - 1).x, bins.get(nusedbins - 1).y);
                    Coord leftExt = new Coord();
                    leftExt.x = leftVirtual[0];
                    leftExt.y = (int) leftVirtual[1];
                    bins.add(0, leftExt);
                    Coord rightExt = new Coord();
                    rightExt.x = rightVirtual[0];
                    rightExt.y = (int) rightVirtual[1];
                    bins.add(rightExt);

                    /*for (int i = 0; i < bins.size(); i++)
                        LOGGER.info("<" + bins.get(i).x + ", " + bins.get(i).y + ">");*/

                    int nParts = nusedbins / QUANTILE_SCALE;
                    double expection = (double) total / nParts;
                    /*LOGGER.info("Total: " + total + " avg: " + expection + " parts: " + nParts);*/
                    List<Coord> gBins = new ArrayList<Coord>();
                    double accd = .0;
                    int current = 0;
                    double localX = bins.get(0).x;
                    double elipsed = .0;
                    for (int i = 0; i < nParts - 1; i++) {
                        Coord cur = new Coord();
                        while (true) {
                            if (current == bins.size() - 1)
                                break;
                            if ((double) (bins.get(current).y + bins.get(current + 1).y) / 2 - elipsed > expection
                                    - accd) {
                                double[] quan = accumulate(expection - accd, bins.get(current).x, bins.get(current).y,
                                        bins.get(current + 1).x, bins.get(current + 1).y, localX, elipsed);
                                cur.x = quan[0];
                                cur.y = (int) expection;
                                localX = quan[0];
                                elipsed += quan[1];
                                gBins.add(cur);
                                /*LOGGER.info("x: " + cur.x + " y: " + cur.y);*/
                                accd = 0;
                                break;
                            } else if ((double) (bins.get(current).y + bins.get(current + 1).y) / 2 - elipsed == expection
                                    - accd) {
                                gBins.add(bins.get(current + 1));
                                gBins.get(gBins.size() - 1).y = (int) expection;
                                /*LOGGER.info("*x: " + gBins.get(gBins.size() - 1).x + " y: "
                                        + gBins.get(gBins.size() - 1).y);*/
                                current++;
                                localX = bins.get(current).x;
                                elipsed = .0;
                                accd = 0;
                                break;
                            } else {
                                accd += (double) (bins.get(current).y + bins.get(current + 1).y) / 2 - elipsed;
                                current++;
                                localX = bins.get(current).x;
                                elipsed = .0;
                            }
                        }
                    }
                    /*for (int i = 0; i < bins.size(); i++)
                        LOGGER.info("<" + bins.get(i).x + ", " + bins.get(i).y + ">");*/
                    gBins.add(new Coord());
                    gBins.get(gBins.size() - 1).x = Double.MAX_VALUE;
                    gBins.get(gBins.size() - 1).y = (int) expection;
                    bins.clear();
                    bins.addAll(gBins);
                    nusedbins = bins.size();
                    nbins = bins.size();
                    break;
                }
                case RAW:
                    break;
            }
            adjustedBound = true;
        }
        for (int i = 0; i < nusedbins; i++) {
            bins.get(i).y = 0;
        }
    }

    @Override
    public E mediate(E left, E right) throws HyracksDataException {
        return quantileToPointable((pointableToQuantile(left) + pointableToQuantile(right)) / 2);
    }

    private Coord interpolate(Coord left, Coord right) {
        //Currently, we support equal height histogram.
        Coord ret = new Coord();
        if (equalHeight) {
            ret.x = (left.x + right.x) / 2;
            ret.y = left.y;
        } else {
            //To be continued.
            ret.x = left.x;
            ret.y = left.y;
        }
        return ret;
    }
}
