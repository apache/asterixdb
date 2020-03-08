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
package org.apache.hyracks.http.server;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe // thread safe for concurrent reads, but concurrent registrations are not supported
public class ServletRegistry {
    private static final Logger LOGGER = LogManager.getLogger();

    private final SortedMap<String, IServlet> servletMap = new TreeMap<>(Comparator.reverseOrder());
    private final Set<IServlet> servlets = new HashSet<>();

    public void register(IServlet let) {
        servlets.add(let);
        for (String path : let.getPaths()) {
            LOGGER.debug("registering servlet {}[{}] with path {}", let, let.getClass().getName(), path);
            IServlet prev = servletMap.put(path, let);
            if (prev != null) {
                throw new IllegalStateException("duplicate servlet mapping! (path = " + path + ", orig = " + prev + "["
                        + prev.getClass().getName() + "], dup = " + let + "[" + let.getClass().getName() + "])");
            }
        }
    }

    public Set<IServlet> getServlets() {
        return Collections.unmodifiableSet(servlets);
    }

    public IServlet getServlet(String uri) {
        String baseUri = HttpUtil.trimQuery(uri);
        return servletMap.entrySet().stream().filter(entry -> match(entry.getKey(), baseUri)).map(Map.Entry::getValue)
                .findFirst().orElse(null);
    }

    static boolean match(String pathSpec, String path) {
        char c = pathSpec.charAt(0);
        if (c == '/') {
            if (pathSpec.equals(path) || (pathSpec.length() == 1 && path.isEmpty())) {
                return true;
            }
            return isPathWildcardMatch(pathSpec, path);
        } else if (c == '*') {
            return path.regionMatches(path.length() - pathSpec.length() + 1, pathSpec, 1, pathSpec.length() - 1);
        }
        return false;
    }

    static boolean isPathWildcardMatch(String pathSpec, String path) {
        final int length = pathSpec.length();
        if (length < 2) {
            return false;
        }
        final int cpl = length - 2;
        final boolean b = pathSpec.endsWith("/*") && path.regionMatches(0, pathSpec, 0, cpl);
        return b && (path.length() == cpl || '/' == path.charAt(cpl));
    }

}
