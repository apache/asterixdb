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
package org.apache.asterix.app.external;

import java.net.URL;
import java.net.URLClassLoader;

public class ExternalLibraryClassLoader extends URLClassLoader {

    private static final ClassLoader bootClassLoader = new ClassLoader(null) {
    };

    public ExternalLibraryClassLoader(URL[] urls, ClassLoader parentClassLoader) {
        super(urls, parentClassLoader);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolveClass) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> clazz = findLoadedClass(name);
            if (clazz == null) {
                try {
                    clazz = bootClassLoader.loadClass(name);
                } catch (ClassNotFoundException ex) {
                    // this is expected path for non-bootclassloader classes
                    try {
                        clazz = findClass(name);
                    } catch (ClassNotFoundException ex2) {
                        // this is expected path for classes not defined in the external library classpath,
                        // finally we try our parent classloader
                        clazz = getParent().loadClass(name);
                    }
                }
            }
            if (resolveClass) {
                resolveClass(clazz);
            }
            return clazz;
        }
    }
}
