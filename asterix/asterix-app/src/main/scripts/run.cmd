@ECHO OFF
SETLOCAL

:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

:: JAVA classpath
:: Use the local variable CLASSPATH to add custom entries (e.g. JDBC drivers) to
:: the classpath. Separate multiple paths with ":". Enclose the value
:: in double quotes. Adding additional files or locations on separate
:: lines makes things clearer.
:: Note: If under running under cygwin use "/cygdrive/c/..." for "C:/..."
:: Example:
::
::     Set the CLASSPATH to a jar file and a directory.  Note that
::     "classes dir" is a directory of class files with a space in the name.
::
:: CLASSPATH="usr/local/Product1/lib/product.jar"
:: CLASSPATH="${CLASSPATH}:../MyProject/classes dir"
::
SET CLASSPATH="@classpath@"

:: JVM parameters
:: If you want to modify the default parameters (e.g. maximum heap size -Xmx)
:: for the Java virtual machine set the local variable JVM_PARAMETERS below
:: Example:
:: JVM_PARAMETERS=-Xms100M -Xmx200M
::
:: Below are the JVM parameters needed to do remote debugging using Intellij
:: IDEA.  Uncomment and then do: JVM_PARAMETERS="$IDEA_REMOTE_DEBUG_PARAMS"
:: IDEA_REMOTE_DEBUG_PARAMS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
::
:: JVM_PARAMETERS=

:: ---------------------------------------------------------------------------
:: Default configuration. Do not modify below this line.
:: ---------------------------------------------------------------------------
:: Application specific parameters

SET MAIN_CLASS=@main.class@
SET JVM_PARAMS=@jvm.params@
SET PROGRAM_PARAMS=@program.params@

:: Try to find java virtual machine
IF NOT DEFINED JAVA (
  IF NOT DEFINED JAVA_HOME SET JAVA="java.exe"
  IF DEFINED JAVA_HOME SET JAVA="%JAVA_HOME%\bin\java.exe"
)

:: Run program
%JAVA% %JVM_PARAMS% %JVM_PARAMETERS% -classpath %CLASSPATH% %MAIN_CLASS% %PROGRAM_PARAMS% %*
