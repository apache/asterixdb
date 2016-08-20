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
package org.apache.asterix.app.cc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.algebra.base.ILangExtension.Language;
import org.apache.asterix.algebra.extension.ExtensionFunctionIdentifier;
import org.apache.asterix.algebra.extension.IAlgebraExtensionManager;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * AsterixDB's implementation of {@code IAlgebraExtensionManager} which takes care of
 * initializing extensions for App and Compilation purposes
 */
public class CompilerExtensionManager implements IAlgebraExtensionManager {

    private static final String ERROR_MESSAGE_ID_CONFLICT = "Two Extensions share the same Id: %1$s";
    public static final String ERROR_MESSAGE_COMPONENT_CONFLICT =
            "Extension Conflict between %1$s and %2$s both extensions extend %3$s";

    private final Map<ExtensionId, IExtension> extensions = new HashMap<>();

    private final IStatementExecutorExtension statementExecutorExtension;
    private final ILangCompilationProvider aqlCompilationProvider;
    private final ILangCompilationProvider sqlppCompilationProvider;
    private final DefaultStatementExecutorFactory defaultQueryTranslatorFactory;

    /**
     * Initialize {@code CompilerExtensionManager} from configuration
     *
     * @param list
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws HyracksDataException
     */
    public CompilerExtensionManager(List<AsterixExtension> list)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, HyracksDataException {
        Pair<ExtensionId, ILangCompilationProvider> aqlcp = null;
        Pair<ExtensionId, ILangCompilationProvider> sqlppcp = null;
        IStatementExecutorExtension see = null;
        defaultQueryTranslatorFactory = new DefaultStatementExecutorFactory(this);

        if (list != null) {
            for (AsterixExtension extensionConf : list) {
                IExtension extension = (IExtension) Class.forName(extensionConf.getClassName()).newInstance();
                extension.configure(extensionConf.getArgs());
                if (extensions.containsKey(extension.getId())) {
                    throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_EXTENSION_CONFLICT,
                            ERROR_MESSAGE_ID_CONFLICT, extension.getId());
                }
                extensions.put(extension.getId(), extension);
                switch (extension.getExtensionKind()) {
                    case STATEMENT_EXECUTOR:
                        see = extendStatementExecutor(see, (IStatementExecutorExtension) extension);
                        break;
                    case LANG:
                        ILangExtension le = (ILangExtension) extension;
                        aqlcp = extendLangCompilationProvider(Language.AQL, aqlcp, le);
                        sqlppcp = extendLangCompilationProvider(Language.SQLPP, sqlppcp, le);
                        break;
                    default:
                        break;
                }
            }
        }
        this.statementExecutorExtension = see;
        this.aqlCompilationProvider = aqlcp == null ? new AqlCompilationProvider() : aqlcp.second;
        this.sqlppCompilationProvider = sqlppcp == null ? new SqlppCompilationProvider() : sqlppcp.second;
    }

    private Pair<ExtensionId, ILangCompilationProvider> extendLangCompilationProvider(Language lang,
            Pair<ExtensionId, ILangCompilationProvider> cp, ILangExtension le) throws HyracksDataException {
        if (cp != null && le.getLangCompilationProvider(lang) != null) {
            throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_EXTENSION_CONFLICT,
                    ERROR_MESSAGE_COMPONENT_CONFLICT, le.getId(), cp.first, lang.toString());
        }
        return (le.getLangCompilationProvider(lang) != null)
                ? new Pair<>(le.getId(), le.getLangCompilationProvider(lang)) : cp;
    }

    private IStatementExecutorExtension extendStatementExecutor(IStatementExecutorExtension qte,
            IStatementExecutorExtension extension) throws HyracksDataException {
        if (qte != null) {
            throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_EXTENSION_CONFLICT,
                    ERROR_MESSAGE_COMPONENT_CONFLICT, qte.getId(), extension.getId(),
                    IStatementExecutorFactory.class.getSimpleName());
        }
        return extension;
    }

    public IStatementExecutorFactory getQueryTranslatorFactory() {
        return statementExecutorExtension == null ? defaultQueryTranslatorFactory
                : statementExecutorExtension.getQueryTranslatorFactory();
    }

    public ILangCompilationProvider getAqlCompilationProvider() {
        return aqlCompilationProvider;
    }

    public ILangCompilationProvider getSqlppCompilationProvider() {
        return sqlppCompilationProvider;
    }

    // TODO(amoudi/yingyi): this is not a good way to extend rewrite rules. introduce re-write rule provider
    @Override
    public boolean unnestToDataScan(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            UnnestOperator unnestOp, ILogicalExpression unnestExpr, AbstractFunctionCallExpression functionCallExpr)
            throws AlgebricksException {
        FunctionIdentifier functionId = functionCallExpr.getFunctionIdentifier();
        if (functionId instanceof ExtensionFunctionIdentifier) {
            ILangExtension extension =
                    (ILangExtension) extensions.get(((ExtensionFunctionIdentifier) functionId).getExtensionId());
            return extension.unnestToDataScan(opRef, context, unnestOp, unnestExpr, functionCallExpr);
        }
        return false;
    }
}
