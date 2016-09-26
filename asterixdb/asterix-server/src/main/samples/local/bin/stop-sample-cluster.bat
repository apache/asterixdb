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

set DIRNAME=%~dp0

pushd %DIRNAME%\..
set CLUSTERDIR=%cd%
cd %CLUSTERDIR%\..\..
set INSTALLDIR=%cd%

call %INSTALLDIR%\bin\${HELPER_COMMAND} get_cluster_state -quiet
if %ERRORLEVEL% NEQ 1 (
  call %INSTALLDIR%\bin\${HELPER_COMMAND} shutdown_cluster_all
) else (
  echo WARNING: sample cluster does not appear to be running, will attempt to wait for
  echo          CCDriver to terminate if running.
)
echo.
powershell "Write-Host "Waiting for CCDriver to terminate..." -nonewline; do { if ($running) { Start-Sleep 1 }; %JAVA_HOME%\bin\jps.exe -v | select-string -pattern ${CC_COMMAND} -quiet -outvariable running | Out-Null; Write-Host "." -nonewline } while ($running)"
echo .done.
goto :END
:ERROR
echo.
popd
exit /B 1

:END
echo.
popd
