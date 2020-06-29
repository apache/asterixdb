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

package org.apache.asterix.external.adapter.factory;

import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.library.JavaLibrary;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public final class ExternalAdapterFactory implements ITypedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private final DataverseName libraryDataverse;

    private final String libraryName;

    private final String className;

    private ARecordType outputType;

    private ARecordType metaType;

    private Map<String, String> configuration;

    private transient ICCServiceContext serviceContext;

    public ExternalAdapterFactory(DataverseName libraryDataverse, String libraryName, String className) {
        this.libraryDataverse = libraryDataverse;
        this.libraryName = libraryName;
        this.className = className;
    }

    @Override
    public void configure(ICCServiceContext serviceContext, Map<String, String> configuration,
            IWarningCollector warningCollector) {
        this.serviceContext = serviceContext;
        this.configuration = configuration;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        //TODO:needs to be specified in the adapter configuration
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceContext.getApplicationContext();
        return IExternalDataSourceFactory.getPartitionConstraints(appCtx, null, 1);
    }

    @Override
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INcApplicationContext appCtx = (INcApplicationContext) serviceCtx.getApplicationContext();
        ILibraryManager libraryManager = appCtx.getLibraryManager();
        ILibrary library = libraryManager.getLibrary(libraryDataverse, libraryName);
        if (ExternalFunctionLanguage.JAVA != library.getLanguage()) {
            throw new HyracksDataException("Unexpected library language: " + library.getLanguage());
        }
        ClassLoader cl = ((JavaLibrary) library).getClassLoader();
        try {
            ITypedAdapterFactory adapterFactory = (ITypedAdapterFactory) cl.loadClass(className).newInstance();
            adapterFactory.setOutputType(outputType);
            adapterFactory.setMetaType(metaType);
            adapterFactory.configure(null, configuration, ctx.getWarningCollector());
            return adapterFactory.createAdapter(ctx, partition);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | AlgebricksException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void setOutputType(ARecordType outputType) {
        this.outputType = outputType;
    }

    @Override
    public ARecordType getOutputType() {
        return outputType;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        this.metaType = metaType;
    }

    @Override
    public ARecordType getMetaType() {
        return metaType;
    }

    @Override
    public String getAlias() {
        return "external:" + className;
    }

    public DataverseName getLibraryDataverse() {
        return libraryDataverse;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public String getClassName() {
        return className;
    }
}
