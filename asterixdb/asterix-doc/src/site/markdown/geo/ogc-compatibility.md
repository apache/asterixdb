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

# OGC Compatibility #

> **Note** — This page supplements [GIS Functions](functions.html) and is the
> reference for the OGC-compatible function names and the new JTS-backed
> functions. The functions and names documented on the existing GIS Functions
> page are unchanged; everything here is *additional*. Users porting queries
> from other OGC-conformant GIS engines can use the compressed `ST_X`-style
> identifiers and the new functions listed below without modifying their SQL.

## <a id="toc">Table of Contents</a> ##

* [Compressed-form OGC name aliases](#aliases)
* [OGC-conformant new functions](#ogc-siblings)
    * [`ST_GeometryType`](#st_geometrytype)
    * [`ST_PointN` (1-indexed)](#st_pointn)
    * [`ST_GeometryN` (1-indexed)](#st_geometryn)
    * [`ST_InteriorRingN` (1-indexed)](#st_interiorringn)
* [New JTS-backed spatial functions](#new-functions)
    * [`ST_ConcaveHull`](#st_concavehull)
    * [`ST_Simplify`](#st_simplify)
    * [`ST_SimplifyPreserveTopology`](#st_simplifypreservetopology)
    * [`ST_PointOnSurface`](#st_pointonsurface)
    * [`ST_LineMerge`](#st_linemerge)
* [Additional OGC-standard functions](#additional-ogc-functions)
    * `ST_NumPoints` (line-only)
    * `ST_Multi`
    * `ST_CollectionExtract`
    * `ST_UnaryUnion`
    * `ST_Normalize`
    * `ST_IsValidReason`
    * `ST_RelateMatch`
* [XYZM (4D) coordinate support](#xyzm)
* [Known compatibility gaps](#gaps)

## <a id="aliases">Compressed-form OGC name aliases</a> ##

OGC-standard function names use CamelCase (e.g. `ST_IsEmpty`, `ST_MakePoint`).
When lowercased they collapse to compressed forms like `st_isempty` and
`st_makepoint`, which do not naturally normalize to AsterixDB's canonical
hyphenated names (`st-is-empty`, `st-make-point`). The following aliases let
users invoke the names they already know:

| Standard form (any case)       | Resolves to            |
|--------------------------------|------------------------|
| `ST_IsEmpty`                   | `st-is-empty`          |
| `ST_IsSimple`                  | `st-is-simple`         |
| `ST_IsClosed`                  | `st-is-closed`         |
| `ST_IsRing`                    | `st-is-ring`           |
| `ST_IsValid`                   | `st-is-valid`          |
| `ST_IsCollection`              | `st-is-collection`     |
| `ST_NPoints`                   | `st-n-points`          |
| `ST_NRings`                    | `st-n-rings`           |
| `ST_NumGeometries`             | `st-num-geometries`    |
| `ST_NumInteriorRings`          | `st-num-interior-rings`|
| `ST_NumInteriorRing`           | `st-num-interior-rings`|
| `ST_CoordDim`                  | `st-coord-dim`         |
| `ST_XMin`, `ST_XMax`           | `st-x-min`, `st-x-max` |
| `ST_YMin`, `ST_YMax`           | `st-y-min`, `st-y-max` |
| `ST_ZMin`, `ST_ZMax`           | `st-z-min`, `st-z-max` |
| `ST_MakePoint`                 | `st-make-point`        |
| `ST_MakeEnvelope`              | `st-make-envelope`     |
| `ST_GeomFromText`              | `st-geom-from-text`    |
| `ST_GeomFromWKB`               | `st-geom-from-wkb`     |
| `ST_GeomFromGeoJSON`           | `st-geom-from-geojson` |
| `ST_LineFromMultiPoint`        | `st-line-from-multipoint` |
| `ST_StartPoint`, `ST_EndPoint` | `st-start-point`, `st-end-point` |
| `ST_ExteriorRing`              | `st-exterior-ring`     |
| `ST_AsText`, `ST_AsWKT`        | `st-as-text`           |
| `ST_AsBinary`, `ST_AsWKB`      | `st-as-binary`         |
| `ST_AsGeoJSON`                 | `st-as-geojson`        |
| `ST_ConvexHull`                | `st-convex-hull`       |
| `ST_SymDifference`             | `st-sym-difference`    |
| `ST_FlipCoordinates`           | `st-flip-coordinates`  |
| `ST_DistanceSphere`            | `st-distance-sphere`   |
| `ST_DistanceSpheroid`          | `st-distance-spheroid` |
| `ST_NumPoints`                 | `st-num-points` (new line-only) |
| `ST_UnaryUnion`                | `st-unary-union`       |
| `ST_IsValidReason`             | `st-is-valid-reason`   |
| `ST_RelateMatch`               | `st-relate-match`      |
| `ST_CollectionExtract`         | `st-collection-extract` |

Existing snake-case forms like `st_is_empty`, `st_make_point`, etc. continue
to work via the underlying `_`-to-`-` normalization — these aliases only
cover the gap where the standard form collapses words without a separator.

`ST_PointN`, `ST_GeometryN`, and `ST_InteriorRingN` are **not** added to this
alias list. They are exposed as new functions with OGC SFA 1-indexed
semantics — see below — because the existing AsterixDB functions are
0-indexed (JTS-native). Routing the compressed form to the existing function
would silently return the wrong element.

## <a id="ogc-siblings">OGC-conformant new functions</a> ##

These are *new* functions that live alongside their AsterixDB counterparts.
Existing functions are unchanged.

### <a id="st_geometrytype">`ST_GeometryType(geometry) -> string`</a>

Returns the geometry type string with the OGC `ST_` prefix: `"ST_Point"`,
`"ST_LineString"`, `"ST_Polygon"`, `"ST_MultiPoint"`,
`"ST_MultiLineString"`, `"ST_MultiPolygon"`, `"ST_GeometryCollection"`.

This is distinct from the existing `geometry_type` function, which returns
the raw JTS form (`"Point"`, `"LineString"`, …) with no prefix.

```sqlpp
SELECT VALUE ST_GeometryType(ST_GeomFromText("LINESTRING(0 0, 1 1)"));
-- "ST_LineString"

SELECT VALUE geometry_type(ST_GeomFromText("LINESTRING(0 0, 1 1)"));
-- "LineString"
```

### <a id="st_pointn">`ST_PointN(linestring, n) -> point`</a>

OGC SFA 1-indexed access to a `LINESTRING` vertex. `ST_PointN(line, 1)`
returns the first point, `ST_PointN(line, length)` returns the last.

Distinct from the existing `st_point_n` which is 0-indexed (JTS-native).

```sqlpp
SELECT VALUE ST_AsText(ST_PointN(ST_GeomFromText("LINESTRING(0 0, 1 1, 2 2)"), 1));
-- "POINT (0 0)"
```

### <a id="st_geometryn">`ST_GeometryN(geometry, n) -> geometry`</a>

OGC SFA 1-indexed access to the Nth component of a geometry. Accepts any
geometry type:

* `GeometryCollection`, `MultiPoint`, `MultiLineString`, `MultiPolygon` —
  returns the Nth component (1-indexed).
* Singular `Point` / `LineString` / `Polygon` — returns the geometry itself
  when `n = 1`.

Distinct from the existing `st_geometry_n` which is 0-indexed and only
accepts plain `GeometryCollection`.

```sqlpp
SELECT VALUE ST_AsText(ST_GeometryN(
    ST_GeomFromText("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((10 10, 11 10, 11 11, 10 11, 10 10)))"),
    2));
-- "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))"
```

Returns `NULL` when `n` is less than 1 or greater than the number of
components. Curved geometries (`MULTICURVE`, `POLYHEDRALSURFACE`, `TIN`)
are not supported by JTS and therefore not by AsterixDB.

### <a id="st_interiorringn">`ST_InteriorRingN(polygon, n) -> linearring`</a>

OGC SFA 1-indexed access to a polygon's Nth interior ring. Distinct from
the existing `st_interior_ring_n` (0-indexed).

```sqlpp
SELECT VALUE ST_AsText(ST_InteriorRingN(
    ST_GeomFromText("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))"),
    1));
-- "LINEARRING (2 2, 8 2, 8 8, 2 8, 2 2)"
```

## <a id="new-functions">New JTS-backed spatial functions</a> ##

### <a id="st_concavehull">`ST_ConcaveHull(geometry, target_percent) -> geometry`</a>

Returns a possibly concave polygon containing all the input points.
`target_percent` is a value in `[0, 1]` controlling the "tightness" of the
hull: `1.0` yields the convex hull; smaller values yield tighter, more
concave shapes.

Implemented via JTS `ConcaveHull.concaveHullByLengthRatio`. The 3-arg variant
with an `allow_holes` flag found in some engines is not yet available.

```sqlpp
ST_ConcaveHull(ST_GeomFromText("MULTIPOINT(0 0, 1 0, 1 1, 0 1, 0.5 0.5)"), 1.0);
```

### <a id="st_simplify">`ST_Simplify(geometry, tolerance) -> geometry`</a>

Douglas–Peucker simplification. Removes vertices that lie within `tolerance`
of the simplified path. May produce invalid polygons when used on closed
rings — use `ST_SimplifyPreserveTopology` if you need ring integrity.

```sqlpp
ST_AsText(ST_Simplify(ST_GeomFromText("LINESTRING(0 0, 5 0.001, 10 0)"), 0.1));
-- "LINESTRING (0 0, 10 0)"
```

### <a id="st_simplifypreservetopology">`ST_SimplifyPreserveTopology(geometry, tolerance) -> geometry`</a>

Topology-preserving simplification (JTS `TopologyPreservingSimplifier`).
Slower than `ST_Simplify`, but never produces invalid output.

### <a id="st_pointonsurface">`ST_PointOnSurface(geometry) -> point`</a>

Returns a `POINT` guaranteed to lie inside the geometry (for polygons) or on
the geometry (for other types). Useful when you need a representative point
inside an arbitrarily shaped polygon (unlike `ST_Centroid`, which may fall
outside a concave shape).

```sqlpp
ST_AsText(ST_PointOnSurface(ST_GeomFromText("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")));
-- "POINT (5 5)"
```

### <a id="st_linemerge">`ST_LineMerge(geometry) -> geometry`</a>

Stitches together a `MULTILINESTRING` (or `GeometryCollection` of
`LineString`s) into the longest continuous lines possible by joining
endpoints. Returns a `LINESTRING` when all components merge into one line,
or a `MULTILINESTRING` otherwise.

```sqlpp
ST_AsText(ST_LineMerge(ST_GeomFromText("MULTILINESTRING((0 0, 1 1), (1 1, 2 2))")));
-- "LINESTRING (0 0, 1 1, 2 2)"
```

## <a id="additional-ogc-functions">Additional OGC-standard functions</a> ##

A second batch of thin JTS wrappers, all standard across OGC-conformant
engines, added for query portability. Each has a compressed-form alias
registered in `CommonFunctionMapUtil` so `ST_NumPoints`, `ST_UnaryUnion`,
etc. resolve without an explicit underscore.

### `ST_NumPoints(linestring) -> int`

OGC SFA line-only vertex count. Returns `NULL` for any non-LineString input.
**Distinct from** the existing `ST_NPoints` / `st-n-points`, which counts
vertices across all geometry types — keep both.

```sqlpp
ST_NumPoints(ST_GeomFromText("LINESTRING(0 0, 1 1, 2 2, 3 3)"));  -- 4
ST_NumPoints(ST_GeomFromText("POINT(0 0)"));                       -- null
```

### `ST_Multi(geometry) -> geometry`

Wraps an atomic `Point` / `LineString` / `Polygon` in the corresponding
`MultiPoint` / `MultiLineString` / `MultiPolygon`. Any geometry that is already
a `Multi*` or `GeometryCollection` is returned unchanged. Useful with
`ST_CollectionExtract` for homogenizing mixed-type collections.

```sqlpp
ST_AsText(ST_Multi(ST_GeomFromText("POINT(1 2)")));  -- "MULTIPOINT ((1 2))"
```

### `ST_CollectionExtract(geometry, type) -> geometry`

Extracts components of the requested type from a `GeometryCollection`,
returning them wrapped as a homogeneous `Multi*`. Type codes: `1` = POINT,
`2` = LINESTRING, `3` = POLYGON. Returns `NULL` when the type code is out
of range, when no components of the requested type are found, or when
either argument is `NULL`.

```sqlpp
ST_AsText(ST_CollectionExtract(
    ST_GeomFromText("GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2), POINT(3 3))"),
    1));
-- "MULTIPOINT ((1 1), (3 3))"
```

### `ST_UnaryUnion(geometry) -> geometry`

Dissolves component boundaries within a single geometry (e.g. overlapping
polygons in a `MultiPolygon` are merged). Implemented via JTS
`UnaryUnionOp.union(Geometry)`.

```sqlpp
ST_AsText(ST_UnaryUnion(ST_GeomFromText(
    "MULTIPOLYGON(((0 0, 2 0, 2 2, 0 2, 0 0)), ((1 1, 3 1, 3 3, 1 3, 1 1)))")));
-- "POLYGON ((2 1, 2 0, 0 0, 0 2, 1 2, 1 3, 3 3, 3 1, 2 1))"
```

### `ST_Normalize(geometry) -> geometry`

Returns a canonicalised form of the geometry — vertex order, ring orientation,
and (where applicable) component ordering. Useful for deterministic
comparisons.

```sqlpp
ST_AsText(ST_Normalize(ST_GeomFromText("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")));
-- "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
```

### `ST_IsValidReason(geometry) -> string`

Returns a textual explanation of why a geometry is invalid, or
`"Valid Geometry"` if it passes validation. Complements `ST_IsValid` (which
returns only a boolean). Implemented via JTS
`IsValidOp.getValidationError().toString()`.

```sqlpp
ST_IsValidReason(ST_GeomFromText("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));
-- "Valid Geometry"
ST_IsValidReason(ST_GeomFromText("POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"));
-- "Self-intersection at or near point (0.5, 0.5, NaN)"
```

### `ST_RelateMatch(matrix_string, pattern_string) -> boolean`

Pure DE-9IM string predicate — given a 9-character intersection matrix string
and a pattern (containing `*`, `T`, `F`, or `0`/`1`/`2`), returns whether the
matrix matches the pattern. Both arguments are strings; no geometry is
involved. Useful for testing the output of `ST_Relate(g1, g2)` against named
predicates.

```sqlpp
ST_RelateMatch("FF0FFF212", "FF*FF****");  -- true
ST_RelateMatch("FF0FFF212", "T********");  -- false
```

## <a id="xyzm">XYZM (4D) coordinate support</a> ##

JTS 1.20 fixed the WKB writer's handling of the M dimension
([JTS PR #734](https://github.com/locationtech/jts/pull/734)), and the
geometry serializer's `getCoordinateDimension` was updated to return `4`
for `CoordinateXYZM`. As a result, points constructed with the 4-argument
`ST_MakePoint(x, y, z, m)` now round-trip through WKB, GeoJSON, and WKT
without losing the M value.

```sqlpp
{
  "z":        ST_Z(ST_MakePoint(1.0, 2.0, 3.0, 4.0)),  -- 3.0
  "m":        ST_M(ST_MakePoint(1.0, 2.0, 3.0, 4.0)),  -- 4.0
  "coorddim": ST_CoordDim(ST_MakePoint(1.0, 2.0, 3.0, 4.0)),  -- 4
  "wkt":      ST_AsText(ST_MakePoint(1.0, 2.0, 3.0, 4.0))     -- "POINT ZM(1 2 3 4)"
};
```

`ST_PointN`, `ST_StartPoint`, and `ST_EndPoint` likewise preserve the Z (and
M, when present) coordinate of the source LineString.

### GeoJSON import / export dimension support

The Jackson-JTS POJO layer used by `ST_GeomFromGeoJSON` / `ST_AsGeoJSON`
follows the GeoJSON convention of variable-length coordinate arrays:

| GeoJSON array     | JTS coordinate type | AsterixDB import | AsterixDB export |
|-------------------|---------------------|------------------|------------------|
| `[x, y]`          | `Coordinate` (XY)   | ✅               | ✅               |
| `[x, y, z]`       | `Coordinate` w/ Z   | ✅               | ✅               |
| `[x, y, z, m]`    | `CoordinateXYZM`    | ✅               | ✅               |
| `[x, y, m]` (XYM) | `CoordinateXYM`     | ❌ ambiguous with XYZ | ❌ M dropped (becomes `[x, y]`) |

The GeoJSON spec (RFC 7946) does not standardize an "XYM" coordinate form;
a three-element array is always interpreted as XYZ. Use the four-element
`[x, y, z, m]` form whenever measure data needs to round-trip through GeoJSON.

### Known gap: WKT-constructed XYM points

`ST_GeomFromText("POINT M (1 2 3)")` is parsed by JTS as `CoordinateXYM`, but
the AsterixDB geometry serializer writes WKB with `outputDimension = 3` and
no explicit M flag, so the M ordinate is lost on the WKB round-trip.
Consequently `ST_M(ST_GeomFromText("POINT M (1 2 3)"))` returns `NaN` and
`ST_AsWKT(...)` emits `"POINT Z(1 2 3)"`. To work with measure data today,
use the four-argument form `ST_MakePoint(x, y, z, m)` — that path constructs
a `CoordinateXYZM` with both Z and M slots, and is written with WKB
`outputDimension = 4`, which `JTS 1.20`'s WKBWriter encodes as EWKB
`Point ZM` and reads back losslessly.

## <a id="gaps">Known compatibility gaps</a> ##

The right-hand column reflects the OGC SFA / SQL/MM standard signature as
typically implemented across mature GIS engines.

| Function           | AsterixDB                                | OGC / common dialect                              |
|--------------------|------------------------------------------|---------------------------------------------------|
| `ST_Transform`     | 3 args (`geom`, `from_srid`, `to_srid`)  | 2 args (`geom`, `to_srid`) — reads source from geom |
| `ST_GeometryType`  | Returns `"ST_Point"` (this page) or `"Point"` (existing `geometry_type`) | Returns `"ST_Point"` |
| `ST_NumPoints`     | Not aliased (semantic mismatch with `st-n-points`) | LineString-only vertex count               |
| `ST_DWithin`       | 3 args                                   | 3 args / 4 args (with `use_spheroid`)             |
| `ST_AsText/AsBinary/AsGeoJSON` | 1 arg                        | 1 / 2 / 3 args (with precision / options)         |
| `ST_Buffer`        | 2 args                                   | 2 / 3 args (with `num_seg_quarter_circle`)        |
| `ST_LineMerge`     | 1 arg                                    | 1 / 2 args (with `directed` flag)                 |
| `ST_Simplify`      | 2 args                                   | 2 / 3 args (with `preserve_collapsed`)            |
| `ST_ConcaveHull`   | 2 args (`target_percent`)                | 2 / 3 args (with `allow_holes`)                   |
| `ST_MakeEnvelope`  | 5 args only (`xmin, ymin, xmax, ymax, srid`) | 4 / 5 args (SRID optional)                   |
| `ST_GeomFromWKB(binary, srid)` | the 2-arg builtin is declared but no descriptor backs it — call will fail at runtime | 2 args supported |
| `ST_SetSRID`       | declared (`st-set-srid`, arity 2) but never registered with a descriptor — unreachable | 2 args supported |
| `ST_NumGeometries(MULTI*)` | returns 1 for `MultiPoint` / `MultiLineString` / `MultiPolygon` because the descriptor only special-cases plain `GeometryCollection`; works correctly for `GEOMETRYCOLLECTION` and atomic types | returns the actual count |
| Curved geometries  | not supported                            | CIRCULARSTRING, COMPOUNDCURVE, etc.               |
| `POLYHEDRALSURFACE`, `TIN` | not supported                    | supported                                         |
