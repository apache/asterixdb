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
package org.apache.asterix.external.library.java;

import org.apache.asterix.external.api.IJType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public abstract class JBuiltinType implements IJType {

    private JBuiltinType() {
        // no op
    }

    public static final JBuiltinType JBOOLEAN = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ABOOLEAN;
        }
    };

    public static final JBuiltinType JBYTE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT8;
        }
    };
    public static final JBuiltinType JCIRCLE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ACIRCLE;
        }
    };
    public static final JBuiltinType JDATE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADATE;
        }
    };
    public static final JBuiltinType JDATETIME = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADATETIME;
        }
    };

    public static final JBuiltinType JDOUBLE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADOUBLE;
        }
    };

    public static final JBuiltinType JDURATION = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ADURATION;
        }
    };

    public static final JBuiltinType JFLOAT = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AFLOAT;
        }
    };

    public static final JBuiltinType JINT = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT32;
        }
    };

    public static final JBuiltinType JINTERVAL = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINTERVAL;
        }
    };

    public static final JBuiltinType JLINE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ALINE;
        }
    };
    public static final JBuiltinType JLONG = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT64;
        }
    };
    public static final JBuiltinType JMISSING = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AMISSING;
        }
    };
    public static final JBuiltinType JNULL = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ANULL;
        }
    };
    public static final JBuiltinType JPOINT = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOINT;
        }
    };
    public static final JBuiltinType JPOINT3D = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOINT3D;
        }
    };
    public static final JBuiltinType JPOLYGON = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.APOLYGON;
        }
    };
    public static final JBuiltinType JRECTANGLE = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ARECTANGLE;
        }
    };
    public static final JBuiltinType JSHORT = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.AINT16;
        }
    };
    public static final JBuiltinType JSTRING = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ASTRING;
        }
    };
    public static final JBuiltinType JTIME = new JBuiltinType() {
        @Override
        public IAType getIAType() {
            return BuiltinType.ATIME;
        }
    };

}
