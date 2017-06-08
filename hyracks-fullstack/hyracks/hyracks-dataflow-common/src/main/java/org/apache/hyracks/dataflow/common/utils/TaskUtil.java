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
package org.apache.hyracks.dataflow.common.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * A Utility class for facilitating common operations used with a hyracks task
 */
public class TaskUtil {
    private TaskUtil() {
    }

    /**
     * get the shared object of a task as a Map<String,Object>
     *
     * @param ctx
     *            the task context
     * @param create
     * @return the task shared map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getSharedMap(IHyracksTaskContext ctx, boolean create) {
        if (ctx.getSharedObject() != null) {
            return (Map<String, Object>) ctx.getSharedObject();
        } else if (create) {
            Map<String, Object> taskMap = new HashMap<>();
            ctx.setSharedObject(taskMap);
            return taskMap;
        } else {
            return null;
        }
    }

    /**
     * put the key value pair in a map task object
     *
     * @param key
     * @param ctx
     * @param object
     */
    public static void put(String key, Object object, IHyracksTaskContext ctx) {
        TaskUtil.getSharedMap(ctx, true).put(key, object);
    }

    /**
     * get a <T> object from the shared map of the task
     *
     * @param key
     * @param ctx
     * @return the value associated with the key casted as T
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(String key, IHyracksTaskContext ctx) {
        Map<String, Object> sharedMap = TaskUtil.getSharedMap(ctx, false);
        return sharedMap == null ? null : (T) sharedMap.get(key);
    }
}
