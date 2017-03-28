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
<#macro license files component="AsterixDB WebUI" location="${asterixAppLocation!}"
                filePrefix="${asterixAppResourcesPrefix!}"
                licenseName="the following license">
   Portions of the ${component}
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

   are available under ${licenseName}:
---
<@indent spaces=3 unpad=true wrap=true>
<#nested>
</@indent>
---
</#macro>
<@license files=["webui/static/js/jquery.min.js", "webui/static/js/jquery.autosize-min.js", "queryui/js/jquery-1.12.4.min.js"]
          licenseName="an MIT-style license">
   Copyright jQuery Foundation and other contributors, https://jquery.org/

   This software consists of voluntary contributions made by many
   individuals. For exact contribution history, see the revision history
   available at https://github.com/jquery/jquery

   The following license applies to all parts of this software except as
   documented below:

   ====

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
   LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
   OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
   WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

   ====

   All files located in the node_modules and external directories are
   externally maintained libraries used by this software which have their
   own licenses; we recommend you read them, as their terms may differ from
   the terms above.
</@license>
<@license files=["webui/static/js/bootstrap.min.js", "webui/static/css/bootstrap-responsive.min.css", "webui/static/css/bootstrap.min.css", "webui/static/img/glyphicons-halflings-white.png", "webui/static/img/glyphicons-halflings.png"]>
   Copyright 2012 Twitter, Inc.
   http://www.apache.org/licenses/LICENSE-2.0.txt

   Credit for webui/static/img/glyphicons-halflings-white.png,
          and webui/static/img/glyphicons-halflings.png

   GLYPHICONS Halflings font is also released as an extension of a Bootstrap
   (www.getbootstrap.com) for free and it is released under the same license as
   Bootstrap. While you are not required to include attribution on your
   Bootstrap-based projects, I would certainly appreciate any form of support,
   even a nice Tweet is enough. Of course if you want, you can say thank you and
   support me by buying more icons on GLYPHICONS.com.
</@license>
<@license component="AsterixDB QueryUI" files=["queryui/js/json-formatter.min.js", "queryui/css/json-formatter.min.css"]>
    Copyright 2014 Mohsen Azimi

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
</@license>
<@license component="AsterixDB QueryUI"
          files=["queryui/js/bootstrap.min.js", "queryui/css/bootstrap.min.css", "queryui/css/bootstrap-theme.min.css",
                 "queryui/css/bootstrap-theme.min.css", "queryui/fonts/glyphicons-halflings-regular.eot",
                 "queryui/fonts/glyphicons-halflings-regular.svg", "queryui/fonts/glyphicons-halflings-regular.ttf",
                 "queryui/fonts/glyphicons-halflings-regular.woff", "queryui/fonts/glyphicons-halflings-regular.woff2"]>
The MIT License (MIT)

Copyright (c) 2011-2015 Twitter, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

queryui/css/bootstrap.min.css also contains normalize.css v3.0.3 (http://github.com/necolas/normalize.css), having the following copyright:
--
Copyright © Nicolas Gallagher and Jonathan Neal

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--
Credit for font files: queryui/fonts/glyphicons-halflings-regular.eot,
                       queryui/fonts/glyphicons-halflings-regular.svg,
                       queryui/fonts/glyphicons-halflings-regular.ttf,
                       queryui/fonts/glyphicons-halflings-regular.woff,
                     and
                       queryui/fonts/glyphicons-halflings-regular.woff2

GLYPHICONS Halflings font is also released as an extension of a Bootstrap
(www.getbootstrap.com) for free and it is released under the same license as
Bootstrap. While you are not required to include attribution on your
Bootstrap-based projects, I would certainly appreciate any form of support,
even a nice Tweet is enough. Of course if you want, you can say thank you and
support me by buying more icons on GLYPHICONS.com.
</@license>

<@license component="AsterixDB QueryUI" files="queryui/js/angular.min.js" licenseName="The MIT License">
    Copyright (c) 2010-2016 Google, Inc. http://angularjs.org

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
</@license>
<@license component="AsterixDB QueryUI" licenseName="The MIT License"
          files=["queryui/js/codemirror.js", "queryui/js/javascript.js", "queryui/css/codemirror.css"]>
    Copyright (C) 2016 by Marijn Haverbeke <marijnh@gmail.com> and others

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
</@license>
<@license component="AsterixDB QueryUI" files="queryui/js/ui-codemirror.js" licenseName="The MIT License">
    Copyright (c) 2012 the AngularUI Team, http://angular-ui.github.com

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
</@license>
<@license component="AsterixDB runtime" files="org/apache/asterix/hivecompat/io/*"
          licenseName="The Apache License, Version 2.0"
          location="${hivecompatLocation!}" filePrefix="${hivecompatPrefix!}">
Source files in asterix-hivecompat are derived from portions of Apache Hive Query Language v0.13.0 (org.apache.hive:hive-exec).
</@license>
<@license component="AsterixDB WebUI" licenseName="The MIT License"
        files=["webui/static/js/jquery.json-viewer.js","webui/static/css/jquery.json-viewer.css"]>
    Copyright (c) 2014 Alexandre Bodelot

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
</@license>
