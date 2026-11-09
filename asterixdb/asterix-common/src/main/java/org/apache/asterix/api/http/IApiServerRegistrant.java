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
package org.apache.asterix.api.http;

/**
 * Extension point for registering servlets on the JSON API server (default port 19002).
 * Implementations are discovered via {@link java.util.ServiceLoader}.
 *
 * To register a servlet, create an implementation of this interface and declare it in:
 * {@code META-INF/services/org.apache.asterix.api.http.IApiServerRegistrant}
 *
 * @see IQueryWebServerRegistrant for the equivalent mechanism on the query web server (port 19006)
 */
public interface IApiServerRegistrant extends IServletRegistrant {
}
