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
package org.apache.hyracks.api.util;

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

import org.apache.hyracks.api.comm.DefaultJavaSerializationProvider;
import org.apache.hyracks.api.comm.IJavaSerializationProvider;

public class JavaSerializationUtils {
    private static IJavaSerializationProvider serProvider = DefaultJavaSerializationProvider.INSTANCE;

    private JavaSerializationUtils() {
    }

    public static byte[] serialize(Serializable jobSpec) throws IOException {
        if (jobSpec instanceof byte[]) {
            return (byte[]) jobSpec;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = serProvider.newObjectOutputStream(baos)) {
            oos.writeObject(jobSpec);
        }
        return baos.toByteArray();
    }

    public static byte[] serialize(Serializable jobSpec, ClassLoader classLoader) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = serProvider.newObjectOutputStream(baos)) {
            ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(classLoader);
                oos.writeObject(jobSpec);
            } finally {
                Thread.currentThread().setContextClassLoader(ctxCL);
            }
        }
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) {
            return null;
        }
        try (ObjectInputStream ois = serProvider.newObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }

    public static Object deserialize(byte[] bytes, ClassLoader classLoader) throws IOException, ClassNotFoundException {
        if (bytes == null) {
            return null;
        }
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try (ObjectInputStream ois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes), classLoader)) {
            return ois.readObject();
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Class<?> loadClass(String className) throws IOException, ClassNotFoundException {
        return Class.forName(className);
    }

    public static void setSerializationProvider(IJavaSerializationProvider serProvider) {
        JavaSerializationUtils.serProvider = serProvider;
    }

    public static IJavaSerializationProvider getSerializationProvider() {
        return serProvider;
    }

    public static void readObject(ObjectInputStream in, Object object) throws IOException, ClassNotFoundException {
        serProvider.readObject(in, object);
    }

    public static void writeObject(ObjectOutputStream out, Object object) throws IOException {
        serProvider.writeObject(out, object);
    }

    private static class ClassLoaderObjectInputStream extends ObjectInputStream {
        private ClassLoader classLoader;

        protected ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader)
                throws IOException, SecurityException {
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
            Class<?>[] classObjs = new Class[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                Class<?> cl = Class.forName(interfaces[i], false, classLoader);
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
