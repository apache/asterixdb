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

package edu.uci.ics.pregelix.api.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * A Writable for ListArray containing instances of a class.
 */
public abstract class ArrayListWritable<M extends Writable> extends ArrayList<M> implements Writable, Configurable {
    /** Used for instantiation */
    private Class<M> refClass = null;
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1L;
    /** Configuration */
    private Configuration conf;
    /** content object pool */
    private List<M> pool = new ArrayList<M>();
    /** how many instance in the pool has been used */
    private int used = 0;
    /** intermediate buffer for copy data element */
    private final ArrayBackedValueStorage intermediateBuffer = new ArrayBackedValueStorage();
    /** intermediate data output */
    private final DataOutput intermediateOutput = intermediateBuffer.getDataOutput();
    /** input stream */
    private final ResetableByteArrayInputStream inputStream = new ResetableByteArrayInputStream();
    /** data input */
    private final DataInput dataInput = new DataInputStream(inputStream);

    /**
     * Using the default constructor requires that the user implement
     * setClass(), guaranteed to be invoked prior to instantiation in
     * readFields()
     */
    public ArrayListWritable() {
    }

    /**
     * clear all the elements
     */
    public void clearElements() {
        this.used = 0;
        this.clear();
    }

    /**
     * Add all elements from another list
     * 
     * @param list
     *            the list of M
     * @return true if successful, else false
     */
    public boolean addAllElements(List<M> list) {
        for (int i = 0; i < list.size(); i++) {
            addElement(list.get(i));
        }
        return true;
    }

    /**
     * Add an element
     * 
     * @param element
     *            M
     * @return true if successful, else false
     */
    public boolean addElement(M element) {
        try {
            intermediateBuffer.reset();
            element.write(intermediateOutput);
            inputStream.setByteArray(intermediateBuffer.getByteArray(), 0);
            M value = allocateValue();
            value.readFields(dataInput);
            add(value);
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * This constructor allows setting the refClass during construction.
     * 
     * @param refClass
     *            internal type class
     */
    public ArrayListWritable(Class<M> refClass) {
        super();
        this.refClass = refClass;
    }

    /**
     * This is a one-time operation to set the class type
     * 
     * @param refClass
     *            internal type class
     */
    public void setClass(Class<M> refClass) {
        if (this.refClass != null) {
            throw new RuntimeException("setClass: refClass is already set to " + this.refClass.getName());
        }
        this.refClass = refClass;
    }

    /**
     * Subclasses must set the class type appropriately and can use
     * setClass(Class<M> refClass) to do it.
     */
    public abstract void setClass();

    public void readFields(DataInput in) throws IOException {
        if (this.refClass == null) {
            setClass();
        }
        used = 0;
        this.clear();
        int numValues = in.readInt(); // read number of values
        for (int i = 0; i < numValues; i++) {
            M value = allocateValue();
            value.readFields(in); // read a value
            add(value); // store it in values
        }
    }

    public void write(DataOutput out) throws IOException {
        int numValues = size();
        out.writeInt(numValues); // write number of values
        for (int i = 0; i < numValues; i++) {
            get(i).write(out);
        }
    }

    public final Configuration getConf() {
        return conf;
    }

    public final void setConf(Configuration conf) {
        this.conf = conf;
        if (this.refClass == null) {
            setClass();
        }
    }

    private M allocateValue() {
        if (used >= pool.size()) {
            M value = ReflectionUtils.newInstance(refClass, conf);
            pool.add(value);
            used++;
            return value;
        } else {
            M value = pool.get(used);
            used++;
            return value;
        }
    }

    public void reset(ArrayIterator<M> iterator) {
        iterator.reset(this);
    }

    public static class ArrayIterator<M> implements Iterator<M> {

        private int pos = 0;
        private List<M> list;

        private void reset(List<M> list) {
            this.list = list;
            pos = 0;
        }

        @Override
        public boolean hasNext() {
            return pos < list.size();
        }

        @Override
        public M next() {
            M item = list.get(pos);
            pos++;
            return item;
        }

        @Override
        public void remove() {
            throw new IllegalStateException("should not be called");
        }

    }
}
