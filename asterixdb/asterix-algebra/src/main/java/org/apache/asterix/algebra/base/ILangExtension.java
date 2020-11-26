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

package org.apache.asterix.algebra.base;

import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.om.functions.IFunctionManager;

/**
 * An interface for language extensions
 */
public interface ILangExtension extends IExtension {

    public enum Language {
        SQLPP
    }

    @Override
    default ExtensionKind getExtensionKind() {
        return ExtensionKind.LANG;
    }

    ILangCompilationProvider getLangCompilationProvider(Language lang);

    default IFunctionManager getFunctionManager() {
        return null;
    }
}
