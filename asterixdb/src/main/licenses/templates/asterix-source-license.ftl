<#--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
-->
<@indent spaces=3>
<#list licenses as license>
  <#if license.url == "http://www.apache.org/licenses/LICENSE-2.0.txt">
${license.content}
    <#break>
  </#if>
</#list>
</@indent>
===
   AsterixDB includes source code with separate copyright notices and
   license terms. Your use of this source code is subject to the terms
   and condition of the following licenses.
===
<#include "source_licenses.ftl">
<#include "source_only_licenses.ftl">
