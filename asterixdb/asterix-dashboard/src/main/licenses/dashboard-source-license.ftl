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
<#macro license files component="${licenseComponent}" location="${licenseLocation}"
filePrefix="${licenseFilePrefix}" licenseName="the following license" extraText="">
   Portions of the ${component}
    <#if !licenseSkipLocations!false>
        <#if location?has_content>
       in: ${location}
        </#if>
       located at:
        <#if files?is_sequence>
            <#list files as file>
                <#if file?counter < files?size>
         ${filePrefix}${file},
                <#else>
       and
         ${filePrefix}${file}
                </#if>
            </#list>
        <#else>
         ${filePrefix}${files}
        </#if>
    </#if>

    <#if "${extraText}" != "">
        <@indent spaces=3 unpad=true wrap=true>
            ${extraText}
        </@indent>

    </#if>
   are available under ${licenseName}:
---
    <@indent spaces=3 unpad=true wrap=true>
        <#nested>
    </@indent>
---
</#macro>
<@license licenseName="The Apache License, Version 2.0" component="AsterixDB Dashboard fonts"
    location="${asterixAppLocation!}" filePrefix="${asterixDashboardResourcesPrefix!'dashboard/'}"
    files=[
    "assets/fonts/material-icons/MaterialIcons-Regular.eot",
    "assets/fonts/material-icons/MaterialIcons-Regular.svg",
    "assets/fonts/material-icons/MaterialIcons-Regular.ttf",
    "assets/fonts/material-icons/MaterialIcons-Regular.woff",
    "assets/fonts/material-icons/MaterialIcons-Regular.woff2"]
    extraText="from material-design-icons (https://github.com/google/material-design-icons)">

<#list licenses as license>
    <#if license.url == "http://www.apache.org/licenses/LICENSE-2.0.txt">
${license.content}
        <#break>
    </#if>
</#list>

</@license>

