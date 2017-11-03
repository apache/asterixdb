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
package org.apache.asterix.utils;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.algebra.base.ILangExtension.Language;
import org.apache.asterix.app.cc.IStatementExecutorExtension;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Provide util methods dealing with extensions
 */
public class ExtensionUtil {

    private ExtensionUtil() {
    }

    /**
     * Verifies no conflict and return the language compilation provider
     *
     * @param lang
     *            the language for the passed compilation provider
     * @param cp
     *            placeholder for compilation provider
     * @param le
     *            user defined extension for compilation provider
     * @return a pair of extension id and extended compilation provider
     * @throws RuntimeDataException
     *             if there was a conflict between two extensions
     */
    public static Pair<ExtensionId, ILangCompilationProvider> extendLangCompilationProvider(Language lang,
            Pair<ExtensionId, ILangCompilationProvider> cp, ILangExtension le) throws RuntimeDataException {
        ILangCompilationProvider lecp = le.getLangCompilationProvider(lang);
        if (cp != null && lecp != null) {
            throw new RuntimeDataException(ErrorCode.EXTENSION_COMPONENT_CONFLICT, le.getId(), cp.first,
                    lang.toString());
        }
        return lecp != null ? new Pair<>(le.getId(), lecp) : cp;
    }

    /**
     * Validate no extension conflict and return function manager extension
     *
     * @param fm
     *            place holder for function manager extension
     * @param le
     *            user defined extension
     * @return the user defined extension
     * @throws RuntimeDataException
     *             if extension conflict was detected
     */
    public static Pair<ExtensionId, IFunctionManager> extendFunctionManager(Pair<ExtensionId, IFunctionManager> fm,
            ILangExtension le) throws RuntimeDataException {
        IFunctionManager lefm = le.getFunctionManager();
        if (fm != null && lefm != null) {
            throw new RuntimeDataException(ErrorCode.EXTENSION_COMPONENT_CONFLICT, le.getId(), fm.first,
                    IFunctionManager.class.getSimpleName());
        }
        return lefm != null ? new Pair<>(le.getId(), lefm) : fm;
    }

    /**
     * Validate no extension conflict and return statement executor extension
     *
     * @param see
     *            place holder for statement executor extension
     * @param extension
     *            user defined extension
     * @return the user defined extension
     * @throws RuntimeDataException
     *             if extension conflict was detected
     */
    public static IStatementExecutorExtension extendStatementExecutor(IStatementExecutorExtension see,
            IStatementExecutorExtension extension) throws RuntimeDataException {
        if (see != null) {
            throw new RuntimeDataException(ErrorCode.EXTENSION_COMPONENT_CONFLICT, see.getId(), extension.getId(),
                    IStatementExecutorFactory.class.getSimpleName());
        }
        return extension;
    }

    /**
     * Validate no extension conflict and extends tuple translator provider
     *
     * @param metadataExtension
     *            place holder for tuple translator provider extension
     * @param mde
     *            user defined metadata extension
     * @return the metadata extension if the extension defines a metadata tuple translator, null otherwise
     * @throws RuntimeDataException
     *             if an extension conflict was detected
     */
    public static IMetadataExtension extendTupleTranslatorProvider(IMetadataExtension metadataExtension,
            IMetadataExtension mde) throws RuntimeDataException {
        if (metadataExtension != null) {
            throw new RuntimeDataException(ErrorCode.EXTENSION_COMPONENT_CONFLICT, metadataExtension.getId(),
                    mde.getId(), IMetadataExtension.class.getSimpleName());
        }
        return mde.getMetadataTupleTranslatorProvider() == null ? null : mde;
    }
}
