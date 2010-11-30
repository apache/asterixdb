/*
 * Copyright 2009-2010 by The Regents of the University of California
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

public class JavaSerializationUtils {
    public static byte[] serialize(Serializable jobSpec) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(jobSpec);
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] bytes, ClassLoader classLoader) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes), classLoader);
        return ois.readObject();
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
    }
}