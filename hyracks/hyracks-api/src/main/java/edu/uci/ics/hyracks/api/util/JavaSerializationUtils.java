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
package edu.uci.ics.hyracks.api.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;

public class JavaSerializationUtils {
    public static byte[] serialize(Serializable jobSpec) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(jobSpec);
        return baos.toByteArray();
    }

    public static byte[] serialize(Serializable jobSpec, ClassLoader classLoader) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            oos.writeObject(jobSpec);
            return baos.toByteArray();
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) {
            return null;
        }
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return ois.readObject();
    }

    public static Object deserialize(byte[] bytes, ClassLoader classLoader) throws IOException, ClassNotFoundException {
        if (bytes == null) {
            return null;
        }
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            ObjectInputStream ois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes), classLoader);
            return ois.readObject();
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Class<?> loadClass(String className) throws IOException, ClassNotFoundException {
        return Class.forName(className);
    }

    private static class ClassLoaderObjectInputStream extends ObjectInputStream {
        private ClassLoader classLoader;

        protected ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException,
                SecurityException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
            return Class.forName(desc.getName(), false, classLoader);
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            ClassLoader nonPublicLoader = null;
            boolean hasNonPublicInterface = false;

            // define proxy in class loader of non-public interface(s), if any
            Class[] classObjs = new Class[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                Class cl = Class.forName(interfaces[i], false, classLoader);
                if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                    if (hasNonPublicInterface) {
                        if (nonPublicLoader != cl.getClassLoader()) {
                            throw new IllegalAccessError("conflicting non-public interface class loaders");
                        }
                    } else {
                        nonPublicLoader = cl.getClassLoader();
                        hasNonPublicInterface = true;
                    }
                }
                classObjs[i] = cl;
            }
            try {
                return Proxy.getProxyClass(hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
            } catch (IllegalArgumentException e) {
                throw new ClassNotFoundException(null, e);
            }
        }
    }
}