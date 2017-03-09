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

goto opts

:usage
echo.
echo Usage: %~nx0 [-f[orce]]:
echo.
echo   -f[orce]  : Forcibly terminates any running ${PRODUCT} processes (after shutting down cluster, if running)
exit /B 0

:kill
echo    Killing %1...
TASKKILL /F /PID %1
echo    %1...killed
exit /B 0

:opts
if "%1" == "" goto postopts

if "%1" == "-f" (
  set force=1
) else if "%1" == "-force" (
  set force=1
) else if "%1" == "-usage" (
  goto :usage
) else if "%1" == "-help" (
  goto :usage
) else if "%1" == "--help" (
  goto :usage
) else if "%1" == "--usage" (
  goto :usage
) else (
  echo ERROR: unknown argument '%1'
  call :usage
  exit /B 1
)
shift
goto opts
:postopts

set DIRNAME=%~dp0

pushd %DIRNAME%\..
set CLUSTERDIR=%cd%
cd %CLUSTERDIR%\..\..
set INSTALLDIR=%cd%

set tempfile="%TEMP%\stop-sample-cluster-%random%"

call "%INSTALLDIR%\bin\${HELPER_COMMAND}" get_cluster_state -quiet
if %ERRORLEVEL% EQU 1 (
  echo WARNING: sample cluster does not appear to be running
  goto :post_shutdown
)
call "%INSTALLDIR%\bin\${HELPER_COMMAND}" shutdown_cluster_all
echo INFO: Waiting for cluster to shutdown...

set tries=0
:wait_loop
set /A tries=%tries% + 1
if "%tries%" == "60" goto :timed_out
wmic process where ^
  "name='java.exe' and CommandLine like '%%org.codehaus.mojo.appassembler.booter.AppassemblerBooter%%' and (CommandLine like '%%app.name=\"%%[cn]c\"%%' or CommandLine like '%%app.name=\"%%ncservice\"%%')" ^
  GET processid >%tempfile% 2> nul

set found=
for /F "skip=1" %%P in ('type %tempfile%') DO set found=1
if "%found%" == "1" (
  call "%INSTALLDIR%\bin\${HELPER_COMMAND}" sleep -timeout 1 -quiet
  goto :wait_loop
)
goto :post_shutdown

:timed_out
echo timed out!

:post_shutdown
echo.

wmic process where ^
  "name='java.exe' and CommandLine like '%%org.codehaus.mojo.appassembler.booter.AppassemblerBooter%%' and (CommandLine like '%%app.name=\"%%[cn]c\"%%' or CommandLine like '%%app.name=\"%%ncservice\"%%')" ^
  GET processid > %tempfile% 2> nul

set found=
for /F "skip=1" %%P in ('type %tempfile%') DO set found=1

if "%found%" == "1" (
  if "%force%" == "1" (
    echo WARNING: ${PRODUCT} processes remain after cluster shutdown; -f[orce] specified, forcibly terminating ${PRODUCT} processes:
    for /F "skip=1" %%P in ('type %tempfile%') DO call :kill %%P
  ) else (
    echo WARNING: ${PRODUCT} processes remain after cluster shutdown; re-run with -f[orce] to forcibly terminate all ${PRODUCT} processes:
    for /F "skip=1" %%P in ('type %tempfile%') DO @echo     - %%P
  )
)
del %tempfile%

goto :END
:ERROR
popd
exit /B 1

:END
popd

