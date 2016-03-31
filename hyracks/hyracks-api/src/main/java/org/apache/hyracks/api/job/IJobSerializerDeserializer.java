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

package org.apache.hyracks.api.job;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;

/**
 * The serializer/deserializer/classloader interface for job/task information such as job specifications, activity graphs and so on.
 *
 * @author yingyib
 */
public interface IJobSerializerDeserializer {

    /**
     * Deserialize the bytes to an object
     *
     * @param bytes
     *            the binary content of an object
     * @return the deserialized object
     * @throws HyracksException
     */
    public Object deserialize(byte[] bytes) throws HyracksException;

    /**
     * Serialize a object into bytes
     *
     * @param object
     *            a Serializable Java object
     * @return
     *         the byte array which contains the binary content of the input object
     * @throws HyracksException
     */
    public byte[] serialize(Serializable object) throws HyracksException;

    /**
     * Load a class by its name
     *
     * @param className
     *            the name of the class
     * @return
     * @throws HyracksException
     */
    public Class<?> loadClass(String className) throws HyracksException;

    /**
     * @param binaryURLs
     * @throws HyracksException
     */
    public void addClassPathURLs(List<URL> binaryURLs) throws HyracksException;

    public ClassLoader getClassLoader() throws HyracksException;

}
