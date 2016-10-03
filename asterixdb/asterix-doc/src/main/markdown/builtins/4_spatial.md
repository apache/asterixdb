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

## <a id="SpatialFunctions">Spatial Functions</a> ##
### create_point ###
 * Syntax:

        create_point(x, y)

 * Creates the primitive type `point` using an `x` and `y` value.
 * Arguments:
   * `x` : a `double` that represents the x-coordinate,
   * `y` : a `double` that represents the y-coordinate.
 * Return Value:
   * a `point` representing the ordered pair (`x`, `y`),
   * `missing` if any argument is a `missing` value,
   * `null` if any argument is a `null` value but no argument is a `missing` value,
   * any other non-double input value will cause a type error.

 * Example:

        { "point": create_point(30.0,70.0) };


 * The expected result is:

        { "point": point("30.0,70.0") }


### create_line ###
 * Syntax:

        create_line(point1, point2)

 * Creates the primitive type `line` using `point1` and `point2`.
 * Arguments:
    * `point1` : a `point` that represents the start point of the line.
    * `point2` : a `point` that represents the end point of the line.
 * Return Value:
    * a spatial `line` created using the points provided in `point1` and `point2`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-point input value will cause a type error.

 * Example:

        { "line": create_line(create_point(30.0,70.0), create_point(50.0,90.0)) };


 * The expected result is:

        { "line": line("30.0,70.0 50.0,90.0") }


### create_rectangle ###
 * Syntax:

        create_rectangle(point1, point2)

 * Creates the primitive type `rectangle` using `point1` and `point2`.
 * Arguments:
    * `point1` : a `point` that represents the lower_left point of the rectangle.
    * `point2` : a `point` that represents the upper_right point of the rectangle.
 * Return Value:
    * a spatial `rectangle` created using the points provided in `point1` and `point2`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-point input value will cause a type error.

 * Example:

        { "rectangle": create_rectangle(create_point(30.0,70.0), create_point(50.0,90.0)) };


 * The expected result is:

        { "rectangle": rectangle("30.0,70.0 50.0,90.0") }


### create_circle ###
 * Syntax:

        create_circle(point, radius)

 * Creates the primitive type `circle` using `point` and `radius`.
 * Arguments:
    * `point` : a `point` that represents the center of the circle.
    * `radius` : a `double` that represents the radius of the circle.
 * Return Value:
    * a spatial `circle` created using the center point and the radius provided in `point` and `radius`.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-point value,
        * or, the second argument is any other non-double value.

 * Example:

        { "circle": create_circle(create_point(30.0,70.0), 5.0) }


 * The expected result is:

        { "circle": circle("30.0,70.0 5.0") }


### create_polygon ###
 * Syntax:

        create_polygon(array)

 * Creates the primitive type `polygon` using the double values provided in the argument `array`.
   Each two consecutive double values represent a point starting from the first double value in the array.
   Note that at least six double values should be specified, meaning a total of three points.
 * Arguments:
     * `array` : an array of doubles representing the points of the polygon.
 * Return Value:
     * a `polygon`, represents a spatial simple polygon created using the points provided in `array`.
     * `missing` if the argument is a `missing` value,
     * `null` if the argument is a `null` value,
     * `missing` if any element in the input array is `missing`,
     * `null` if any element in the input array is `null` but no element in the input array is `missing`,
     * any other non-array input value or non-double element in the input array will cause a type error.


 * Example:

        { "polygon": create_polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0]) };


 * The expected result is:

        { "polygon": polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0") }


### get_x/get_y ###
 * Syntax:

        get_x(point) or get_y(point)

 * Returns the x or y coordinates of a point `point`.
 * Arguments:
    * `point` : a `point`.
 * Return Value:
    * a `double` representing the x or y coordinates of the point `point`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-point input value will cause a type error.

 * Example:

        { "x_coordinate": get_x(create_point(2.3,5.0)), "y_coordinate": get_y(create_point(2.3,5.0)) };


 * The expected result is:

        { "x_coordinate": 2.3, "y_coordinate": 5.0 }


### get_points ###
 * Syntax:

        get_points(spatial_object)

 * Returns an ordered array of the points forming the spatial object `spatial_object`.
 * Arguments:
    * `spatial_object` : a `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * an `array` of the points forming the spatial object `spatial_object`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-spatial-object input value will cause a type error.

 * Example:

        get_points(create_polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0]))

 * The expected result is:

        [ point("1.0,1.0"), point("2.0,2.0"), point("3.0,3.0"), point("4.0,4.0") ]


### get_center/get_radius ###
 * Syntax:

        get_center(circle_expression) or get_radius(circle_expression)

 * Returns the center and the radius of a circle `circle_expression`, respectively.
 * Arguments:
    * `circle_expression` : a `circle`.
 * Return Value:
    * a `point` or `double`, represent the center or radius of the circle `circle_expression`.
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-circle input value will cause a type error.

 * Example:

        {
          "circle_radius": get_radius(create_circle(create_point(6.0,3.0), 1.0)),
          "circle_center": get_center(create_circle(create_point(6.0,3.0), 1.0))
        };


 * The expected result is:

        { "circle_radius": 1.0, "circle_center": point("6.0,3.0") }



### spatial_distance ###
 * Syntax:

        spatial_distance(point1, point2)

 * Returns the Euclidean distance between `point1` and `point2`.
 * Arguments:
    * `point1` : a `point`.
    * `point2` : a `point`.
 * Return Value:
    * a `double` as the Euclidean distance between `point1` and `point2`.
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-point input value will cause a type error.

 * Example:

        spatial_distance(point("47.44,80.65"), create_point(30.0,70.0));


 * The expected result is:

        20.434678857275934

### spatial_area ###
 * Syntax:

        spatial_area(spatial_2d_expression)

 * Returns the spatial area of `spatial_2d_expression`.
 * Arguments:
    * `spatial_2d_expression` : a `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * a `double` representing the area of `spatial_2d_expression`.
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-2d-spatial-object will cause a type error.

 * Example:

        spatial_area(create_circle(create_point(0.0,0.0), 5.0));


 * The expected result is:

        78.53981625


### spatial_intersect ###
 * Syntax:

        spatial_intersect(spatial_object1, spatial_object2)

 * Checks whether `@arg1` and `@arg2` spatially intersect each other.
 * Arguments:
    * `spatial_object1` : a `point`, `line`, `rectangle`, `circle`, or `polygon`.
    * `spatial_object2` : a `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * a `boolean` representing whether `spatial_object1` and `spatial_object2` spatially overlap with each other,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-spatial-object input value will cause a type error.

 * Example:

        spatial_intersect(point("39.28,70.48"), create_rectangle(create_point(30.0,70.0), create_point(40.0,80.0)));


 * The expected result is:

        true

### spatial_cell ###
 * Syntax:

        spatial_cell(point1, point2, x_increment, y_increment)

 * Returns the grid cell that `point1` belongs to.
 * Arguments:
    * `point1` : a `point` representing the point of interest that its grid cell will be returned.
    * `point2` : a `point` representing the origin of the grid.
    * `x_increment` : a `double`, represents X increments.
    * `y_increment` : a `double`, represents Y increments.
 * Return Value:
    * a `rectangle` representing the grid cell that `point1` belongs to,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first or second argument is any other non-point value,
        * or, the second or third argument is any other non-double value.

 * Example:

        spatial_cell(point("39.28,70.48"), create_point(20.0,50.0), 5.5, 6.0);


 * The expected result is:

        rectangle("36.5,68.0 42.0,74.0");

