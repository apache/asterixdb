@rem /*
@rem Copyright 2009-2013 by The Regents of the University of California
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem you may obtain a copy of the License from
@rem 
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem 
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem */
@REM ----------------------------------------------------------------------------
@REM  Copyright 2001-2006 The Apache Software Foundation.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM       http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM ----------------------------------------------------------------------------
@REM
@REM   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
@REM   reserved.

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\lib

cp $BASEDIR"\..\a-hadoop-patch.jar "$REPO"\

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\a-hadoop-patch.jar;"%REPO%"\pregelix-api-0.0.1-SNAPSHOT.jar;"%REPO%"\hyracks-dataflow-common-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-api-0.2.2-SNAPSHOT.jar;"%REPO%"\json-20090211.jar;"%REPO%"\httpclient-4.1-alpha2.jar;"%REPO%"\httpcore-4.1-beta1.jar;"%REPO%"\commons-logging-1.1.1.jar;"%REPO%"\commons-codec-1.3.jar;"%REPO%"\args4j-2.0.12.jar;"%REPO%"\hyracks-ipc-0.2.2-SNAPSHOT.jar;"%REPO%"\commons-lang3-3.1.jar;"%REPO%"\hyracks-data-std-0.2.2-SNAPSHOT.jar;"%REPO%"\hadoop-core-0.20.2.jar;"%REPO%"\commons-cli-1.2.jar;"%REPO%"\xmlenc-0.52.jar;"%REPO%"\commons-httpclient-3.0.1.jar;"%REPO%"\commons-net-1.4.1.jar;"%REPO%"\oro-2.0.8.jar;"%REPO%"\jetty-6.1.14.jar;"%REPO%"\jetty-util-6.1.14.jar;"%REPO%"\servlet-api-2.5-6.1.14.jar;"%REPO%"\jasper-runtime-5.5.12.jar;"%REPO%"\jasper-compiler-5.5.12.jar;"%REPO%"\jsp-api-2.1-6.1.14.jar;"%REPO%"\jsp-2.1-6.1.14.jar;"%REPO%"\core-3.1.1.jar;"%REPO%"\ant-1.6.5.jar;"%REPO%"\commons-el-1.0.jar;"%REPO%"\jets3t-0.7.1.jar;"%REPO%"\kfs-0.3.jar;"%REPO%"\hsqldb-1.8.0.10.jar;"%REPO%"\pregelix-dataflow-std-0.0.1-SNAPSHOT.jar;"%REPO%"\pregelix-dataflow-std-base-0.0.1-SNAPSHOT.jar;"%REPO%"\hyracks-dataflow-std-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-dataflow-hadoop-0.2.2-SNAPSHOT.jar;"%REPO%"\dcache-client-0.0.1.jar;"%REPO%"\jetty-client-8.0.0.M0.jar;"%REPO%"\jetty-http-8.0.0.RC0.jar;"%REPO%"\jetty-io-8.0.0.RC0.jar;"%REPO%"\jetty-util-8.0.0.RC0.jar;"%REPO%"\hyracks-storage-am-common-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-storage-common-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-storage-am-btree-0.2.2-SNAPSHOT.jar;"%REPO%"\btreehelper-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-control-cc-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-control-common-0.2.2-SNAPSHOT.jar;"%REPO%"\commons-io-1.3.1.jar;"%REPO%"\jetty-server-8.0.0.RC0.jar;"%REPO%"\servlet-api-3.0.20100224.jar;"%REPO%"\jetty-continuation-8.0.0.RC0.jar;"%REPO%"\jetty-webapp-8.0.0.RC0.jar;"%REPO%"\jetty-xml-8.0.0.RC0.jar;"%REPO%"\jetty-servlet-8.0.0.RC0.jar;"%REPO%"\jetty-security-8.0.0.RC0.jar;"%REPO%"\wicket-core-1.5.2.jar;"%REPO%"\wicket-util-1.5.2.jar;"%REPO%"\slf4j-api-1.6.1.jar;"%REPO%"\wicket-request-1.5.2.jar;"%REPO%"\slf4j-jcl-1.6.3.jar;"%REPO%"\hyracks-control-nc-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-net-0.2.2-SNAPSHOT.jar;"%REPO%"\hyracks-hadoop-compat-0.2.2-SNAPSHOT.jar;"%REPO%"\pregelix-dataflow-0.0.1-SNAPSHOT.jar;"%REPO%"\pregelix-runtime-0.0.1-SNAPSHOT.jar;"%REPO%"\hadoop-test-0.20.2.jar;"%REPO%"\ftplet-api-1.0.0.jar;"%REPO%"\mina-core-2.0.0-M5.jar;"%REPO%"\ftpserver-core-1.0.0.jar;"%REPO%"\ftpserver-deprecated-1.0.0-M2.jar;"%REPO%"\javax.servlet-api-3.0.1.jar;"%REPO%"\pregelix-core-0.0.1-SNAPSHOT.jar
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS%  -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="pregelix" -Dapp.repo="%REPO%" -Dapp.home="%BASEDIR%" -Dbasedir="%BASEDIR%" org.apache.hadoop.util.RunJar %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=%ERRORLEVEL%

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@REM If error code is set to 1 then the endlocal was done already in :error.
if %ERROR_CODE% EQU 0 @endlocal


:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%