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
package org.apache.asterix.runtime.evaluators.functions.spatial;

import org.apache.asterix.om.base.ARectangle;

public class SpatialLogic {

    public SpatialLogic() {

    }

    public boolean intersects(ARectangle rect1, ARectangle rect2) {
        // If one rectangle is on left side of other
        if ((rect1.getP1().getX() > rect2.getP2().getX()) || (rect2.getP1().getX() > rect1.getP2().getX())) {
            return false;
        }

        // If one rectangle is above other
        if ((rect1.getP1().getY() > rect2.getP2().getY()) || (rect2.getP1().getY() > rect1.getP2().getY())) {
            return false;
        }

        return true;
    }

    public boolean intersectsAtReferenceTile(ARectangle rect1, ARectangle rect2, ARectangle mbr, int tileId, int rows,
            int columns) {
        if (intersects(rect1, rect2)) {
            // Compute the reference point
            double x = Math.max(rect1.getP1().getX(), rect2.getP1().getX());
            double y = Math.max(rect1.getP1().getY(), rect2.getP1().getY());

            // Compute the tile ID of the reference point
            double minX = mbr.getP1().getX();
            double minY = mbr.getP1().getY();
            double maxX = mbr.getP2().getX();
            double maxY = mbr.getP2().getY();
            int row = (int) Math.ceil((y - minY) * rows / (maxY - minY));
            int col = (int) Math.ceil((x - minX) * columns / (maxX - minX));

            row = Math.min(Math.max(1, row), rows * columns);
            col = Math.min(Math.max(1, col), rows * columns);

            int referenceTileId = (row - 1) * columns + col;

            return referenceTileId == tileId;
        }

        return false;
    }
}
