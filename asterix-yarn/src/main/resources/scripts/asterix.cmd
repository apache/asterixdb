@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

setlocal enabledelayedexpansion

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

set DEFAULT_LIBEXEC_DIR=%HADOOP_BIN_PATH%\..\libexec
if not defined HADOOP_LIBEXEC_DIR (
  set HADOOP_LIBEXEC_DIR=%DEFAULT_LIBEXEC_DIR%
)

:main

  set CLASS=edu.uci.ics.asterix.aoya.AsterixYARNClient

  @rem JAVA and JAVA_HEAP_MAX and set in hadoop-config.cmd

  if defined YARN_HEAPSIZE (
    @rem echo run with Java heapsize %YARN_HEAPSIZE%
    set JAVA_HEAP_MAX=-Xmx%YARN_HEAPSIZE%m
  )

  @rem CLASSPATH initially contains HADOOP_CONF_DIR & YARN_CONF_DIR
  if not defined HADOOP_CONF_DIR (
    echo No HADOOP_CONF_DIR set. 
    echo Please specify it either in yarn-env.cmd or in the environment.
    goto :eof
  )

  set CLASSPATH=%HADOOP_CONF_DIR%;%YARN_CONF_DIR%;%CLASSPATH%;Z:\lib\*

  @rem for developers, add Hadoop classes to CLASSPATH
  if exist %HADOOP_YARN_HOME%\yarn-api\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-api\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-mapreduce\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-mapreduce\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-master-worker\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-master-worker\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\build\test\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\test\classes
  )

  if exist %HADOOP_YARN_HOME%\build\tools (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\tools
  )

  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\*
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_LIB_JARS_DIR%\*

  

  if defined JAVA_LIBRARY_PATH (
    set YARN_OPTS=%YARN_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
  )

  set java_arguments=%JAVA_HEAP_MAX% %YARN_OPTS% -classpath %CLASSPATH% %CLASS%
  call java %java_arguments% %*

goto :eof

endlocal
