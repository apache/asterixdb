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
package org.apache.asterix.external.library;

import static org.apache.asterix.external.library.PythonLibraryTCPSocketEvaluator.SITE_PACKAGES;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.ILibraryEvaluator;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.ipc.impl.IPCSystem;

public class PythonLibraryEvaluatorFactory {

    private ILibraryManager libraryManager;
    private IPCSystem ipcSys;
    private File pythonPath;
    private IHyracksTaskContext ctx;
    private ExternalFunctionResultRouter router;
    private String sitePackagesPath;
    private List<String> pythonArgs;
    private Map<String, String> pythonEnv;

    private boolean domainSockEnable;

    public PythonLibraryEvaluatorFactory(IHyracksTaskContext ctx) throws AsterixException {
        this.ctx = ctx;
        String dsPath =
                ctx.getJobletContext().getServiceContext().getAppConfig().getString(NCConfig.Option.PYTHON_DS_PATH);
        config(dsPath == null ? null : Path.of(dsPath));
        libraryManager = ((INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext())
                .getLibraryManager();
        if (!domainSockEnable) {
            router = libraryManager.getRouter();
            ipcSys = libraryManager.getIPCI();
            IApplicationConfig appCfg = ctx.getJobletContext().getServiceContext().getAppConfig();
            String pythonPathCmd = appCfg.getString(NCConfig.Option.PYTHON_CMD);
            boolean findPython = appCfg.getBoolean(NCConfig.Option.PYTHON_CMD_AUTOLOCATE);
            pythonArgs = new ArrayList<>();
            if (pythonPathCmd == null) {
                if (findPython) {
                    //if absolute path to interpreter is not specified, try to use environmental python
                    pythonPathCmd = "/usr/bin/env";
                    pythonArgs.add("python3");
                } else {
                    throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                            "Python interpreter not specified or domain socket not found, and "
                                    + NCConfig.Option.PYTHON_CMD_AUTOLOCATE.ini() + " is false");
                }
            }
            pythonEnv = new HashMap<>();
            String[] envRaw = appCfg.getStringArray((NCConfig.Option.PYTHON_ENV));
            if (envRaw != null) {
                for (String rawEnvArg : envRaw) {
                    //TODO: i think equals is shared among all unixes and windows. but it needs verification
                    if (rawEnvArg.length() < 1) {
                        continue;
                    }
                    String[] rawArgSplit = rawEnvArg.split("(?<!\\\\)=", 2);
                    if (rawArgSplit.length < 2) {
                        throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                                "Invalid environment variable format detected.");
                    }
                    pythonEnv.put(rawArgSplit[0], rawArgSplit[1]);
                }
            }
            pythonPath = new File(pythonPathCmd);
            List<String> sitePkgs = new ArrayList<>();
            sitePkgs.add(SITE_PACKAGES);
            String[] addlSitePackages = appCfg.getStringArray((NCConfig.Option.PYTHON_ADDITIONAL_PACKAGES));
            for (String sitePkg : addlSitePackages) {
                if (sitePkg.length() > 0) {
                    sitePkgs.add(sitePkg);
                }
            }
            if (appCfg.getBoolean(NCConfig.Option.PYTHON_USE_BUNDLED_MSGPACK)) {
                sitePkgs.add("ipc" + File.separator + SITE_PACKAGES + File.separator);
            }
            String[] pythonArgsRaw = appCfg.getStringArray(NCConfig.Option.PYTHON_ARGS);
            if (pythonArgsRaw != null) {
                for (String arg : pythonArgsRaw) {
                    if (arg.length() > 0) {
                        pythonArgs.add(arg);
                    }
                }
            }
            StringBuilder sitePackagesPathBuilder = new StringBuilder();
            for (int i = 0; i < sitePkgs.size() - 1; i++) {
                sitePackagesPathBuilder.append(sitePkgs.get(i));
                sitePackagesPathBuilder.append(File.pathSeparator);
            }
            sitePackagesPathBuilder.append(sitePkgs.get(sitePkgs.size() - 1));
            sitePackagesPath = sitePackagesPathBuilder.toString();
        }
    }

    public ILibraryEvaluator getEvaluator(IExternalFunctionInfo fnInfo, SourceLocation sourceLoc)
            throws IOException, AsterixException {
        if (domainSockEnable) {
            return PythonLibraryDomainSocketEvaluator.getInstance(fnInfo, libraryManager, ctx,
                    ctx.getWarningCollector(), sourceLoc);
        } else {
            return PythonLibraryTCPSocketEvaluator.getInstance(fnInfo, libraryManager, router, ipcSys, pythonPath, ctx,
                    sitePackagesPath, pythonArgs, pythonEnv, ctx.getWarningCollector(), sourceLoc);
        }
    }

    private void config(Path sockPath) throws AsterixException {
        if (sockPath == null) {
            domainSockEnable = false;
            return;
        }
        Runtime rt = Runtime.getRuntime();
        if (rt.version().feature() >= 17 && SystemUtils.IS_OS_LINUX) {
            if (Files.exists(sockPath)) {
                domainSockEnable = true;
            } else {
                throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                        "Domain socket was not found at specified path");
            }
        } else {
            throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                    "Domain socket path specified, but Java version is below 17 or OS is not Linux");
        }
    }
}
