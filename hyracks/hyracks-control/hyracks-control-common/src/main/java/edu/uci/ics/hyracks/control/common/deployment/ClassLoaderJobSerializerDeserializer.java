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

package edu.uci.ics.hyracks.control.common.deployment;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

/**
 * This is the IJobSerializerDeserializer implementation for jobs with dynamic deployed jars.
 * 
 * @author yingyib
 *
 */
public class ClassLoaderJobSerializerDeserializer implements IJobSerializerDeserializer {

    private URLClassLoader classLoader;

    @Override
    public Object deserialize(byte[] jsBytes) throws HyracksException {
        try {
            if (classLoader == null) {
                return JavaSerializationUtils.deserialize(jsBytes);
            }
            return JavaSerializationUtils.deserialize(jsBytes, classLoader);
        } catch (Exception e) {
            throw new HyracksException(e);
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
            throw new HyracksException(e);
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
                /** crate a new classloader */
                URL[] urls = binaryURLs.toArray(new URL[binaryURLs.size()]);
                classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
            } else {
                /** add URLs to the existing classloader */
                Object[] urls = binaryURLs.toArray(new URL[binaryURLs.size()]);
                Method method = classLoader.getClass().getDeclaredMethod("addURL", new Class[] { URL.class });
                method.setAccessible(true);
                method.invoke(classLoader, urls);
            }
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public Class<?> loadClass(String className) throws HyracksException {
        try {
            return classLoader.loadClass(className);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public ClassLoader getClassLoader() throws HyracksException {
        return classLoader;
    }
}
