@REM ------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM   http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ------------------------------------------------------------

@echo off
setlocal
if NOT DEFINED JAVA_HOME (
  echo ERROR: JAVA_HOME not defined
  goto :ERROR
)
REM ensure JAVA_HOME has no spaces nor quotes, since appassembler can't handle them
set JAVA_HOME=%JAVA_HOME:"=%
for %%I in ("%JAVA_HOME%") do (
  set JAVA_HOME=%%~sI
)

set JAVACMD=%JAVA_HOME%\bin\java

REM TODO(mblow): check java version, spaces in CWD

set DIRNAME=%~dp0

pushd %DIRNAME%\..
set CLUSTERDIR=%cd%
cd %CLUSTERDIR%\..\..
set INSTALLDIR=%cd%
set LOGSDIR=%CLUSTERDIR%\logs

echo CLUSTERDIR=%CLUSTERDIR%
echo INSTALLDIR=%INSTALLDIR%
echo LOGSDIR=%LOGSDIR%
echo.
cd %CLUSTERDIR%
if NOT EXIST %LOGSDIR% (
  mkdir %LOGSDIR%
)
call %INSTALLDIR%\bin\${HELPER_COMMAND} get_cluster_state -quiet

IF %ERRORLEVEL% EQU 0 (
  echo ERROR: sample cluster address [localhost:${LISTEN_PORT}] already in use
  goto :ERROR
)

REM TODO(mblow): append timestamp to logs when starting

echo Starting sample cluster...

start /MIN "blue-nc" cmd /c "echo See output in %LOGSDIR%\blue-service.log && %INSTALLDIR%\bin\${NC_SERVICE_COMMAND} -logdir - -config-file %CLUSTERDIR%\conf\blue.conf >> %LOGSDIR%\blue-service.log 2>&1"
start /MIN "red-nc" cmd /c "echo See output in %LOGSDIR%\red-service.log && %INSTALLDIR%\bin\${NC_SERVICE_COMMAND} -logdir - >> %LOGSDIR%\red-service.log 2>&1"
start /MIN "cc" cmd /c "echo See output in %LOGSDIR%\cc.log && %INSTALLDIR%\bin\${CC_COMMAND} -config-file %CLUSTERDIR%\conf\cc.conf >>%LOGSDIR%\cc.log 2>&1"

echo.
call %INSTALLDIR%\bin\${HELPER_COMMAND} wait_for_cluster -timeout 30
if %ERRORLEVEL% EQU 0 (
  goto :END
)

:ERROR
echo.
popd
endlocal
exit /B 1

:END
echo.
popd
endlocal
