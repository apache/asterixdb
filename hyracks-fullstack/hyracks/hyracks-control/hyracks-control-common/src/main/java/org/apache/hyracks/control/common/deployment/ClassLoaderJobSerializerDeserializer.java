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

package org.apache.hyracks.control.common.deployment;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobSerializerDeserializer;
import org.apache.hyracks.api.util.JavaSerializationUtils;

/**
 * This is the IJobSerializerDeserializer implementation for jobs with dynamic deployed jars.
 *
 * @author yingyib
 */
public class ClassLoaderJobSerializerDeserializer implements IJobSerializerDeserializer {

    private MutableURLClassLoader classLoader;

    @Override
    public Object deserialize(byte[] jsBytes) throws HyracksException {
        try {
            if (classLoader == null) {
                return JavaSerializationUtils.deserialize(jsBytes);
            }
            return JavaSerializationUtils.deserialize(jsBytes, classLoader);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public byte[] serialize(Serializable jobSpec) throws HyracksException {
        try {
            if (classLoader == null) {
                return JavaSerializationUtils.serialize(jobSpec);
            }
            return JavaSerializationUtils.serialize(jobSpec, classLoader);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public void addClassPathURLs(List<URL> binaryURLs) throws HyracksException {
        Collections.sort(binaryURLs, new Comparator<URL>() {
            @Override
            public int compare(URL o1, URL o2) {
                return o1.getFile().compareTo(o2.getFile());
            }
        });
        try {
            if (classLoader == null) {
                /** create a new classloader */
                URL[] urls = binaryURLs.toArray(new URL[binaryURLs.size()]);
                classLoader = new MutableURLClassLoader(urls, this.getClass().getClassLoader());
            } else {
                /** add URLs to the existing classloader */
                for (URL url : binaryURLs) {
                    classLoader.addURL(url);
                }
            }
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public Class<?> loadClass(String className) throws HyracksException {
        try {
            return classLoader.loadClass(className);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public ClassLoader getClassLoader() throws HyracksException {
        return classLoader;
    }

    @Override
    public String toString() {
        return classLoader.toString();
    }

    private static class MutableURLClassLoader extends URLClassLoader {

        public MutableURLClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        protected void addURL(URL url) {
            super.addURL(url);
        }
    }
}
