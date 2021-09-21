<!--
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
 !-->

## <a id="spatial_joins">Spatial Joins</a>
AsterixDB supports efficient spatial join query performance.
In particular, it will execute the [Partition Based Spatial-Merge Join](http://pages.cs.wisc.edu/~dewitt/includes/paradise/spjoin.pdf).
(PBSM) algorithm for the join queries with a spatial function in join condition.

Supported spatial functions:
- `spatial_intersect(ARectangle, ARectangle)`.
- ESRI's spatial functions: `st_intersects()`, `st_overlaps()`, `st_touches()`, `st_contains()`, `st_crosses()`, `st_within()`, `st_distance()`.

Once the join condition contains a supported spatial function, users do not need to do any further action to trigger this efficient query plan.

### <a id="spatial_partitioning_hint">Using a spatial partitioning hint</a>
PBSM algorithm requires the following information to partition data into a grid:
- The MBR of two input datasets.
- Number of rows and columns of the grid.

By default, the MBR will be computed at running time and the grid size is 100x100.
However, users can also set other values for these parameters using spatial partitioning hint.

##### Spatial partitioning hint example
In this example, assume that MBR of two input datasets is (-180.0, -83.0, 180.0, 90.0) and grid size is 10x10.


    /*+ spatial-partitioning(-180.0, -83.0, 180.0, 90.0, 10, 10) */


##### Spatial partitioning hint in a query

    DROP DATAVERSE test IF EXISTS;
    CREATE DATAVERSE test;
    USE test;

    -- Make GeomType
    CREATE TYPE GeomType as closed {
        id: int32,
        geom: rectangle
    };

    -- Make Park dataset
    CREATE DATASET ParkSet (GeomType) primary key id;

    -- Make Lake dataset
    CREATE DATASET LakeSet (GeomType) primary key id;

    SELECT COUNT(*) FROM ParkSet AS ps, LakeSet AS ls
    WHERE /*+ spatial-partitioning(-180.0, -83.0, 180.0, 90.0, 10, 10) */ spatial_intersect(ps.geom, ls.geom);
