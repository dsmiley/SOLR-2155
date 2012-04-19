/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package solr2155.lucene.spatial.geometry.shape;

import org.apache.lucene.spatial.DistanceUtils;

/**
 * An ellipse-like geometry based on the haversine formula with a supplied earth radius.
 */
public final class PointDistanceGeom implements Geometry2D {
  private final Point2D point;
  private final double distance;
  private final double radius;
  private transient Rectangle enclosingBox1, enclosingBox2;//calculated & cached (2nd is usually null)

  public PointDistanceGeom(double lat, double lon, double dist, double radius) {
    this.point = new Point2D(lon,lat);
    this.distance = dist;
    this.radius = radius;
    calcEnclosingBoxes();
  }

  public double getDistance() {
    return distance;
  }

  private void calcEnclosingBoxes() {
    //!! code copied from LatLonType.createSpatialQuery(); this should be consolidated
    final int LAT = 0;
    final int LONG = 1;

    double[] point = new double[]{this.point.getY(),this.point.getX()};
    point[0] = point[0] * DistanceUtils.DEGREES_TO_RADIANS;
    point[1] = point[1] * DistanceUtils.DEGREES_TO_RADIANS;
    double[] tmp = new double[2];
    //these calculations aren't totally accurate, but it should be good enough
    //TODO: Optimize to do in single calculations.  Would need to deal with poles, prime meridian, etc.
    double [] north = DistanceUtils.pointOnBearing(point[LAT], point[LONG], distance, 0, tmp, radius);
    //This returns the point as radians, but we need degrees b/c that is what the field is stored as
    double ur_lat = north[LAT] * DistanceUtils.RADIANS_TO_DEGREES;//get it now, as we are going to reuse tmp
    double [] east = DistanceUtils.pointOnBearing(point[LAT], point[LONG], distance, DistanceUtils.DEG_90_AS_RADS, tmp, radius);
    double ur_lon = east[LONG] * DistanceUtils.RADIANS_TO_DEGREES;
    double [] south = DistanceUtils.pointOnBearing(point[LAT], point[LONG], distance, DistanceUtils.DEG_180_AS_RADS, tmp, radius);
    double ll_lat = south[LAT] * DistanceUtils.RADIANS_TO_DEGREES;
    double [] west = DistanceUtils.pointOnBearing(point[LAT], point[LONG], distance, DistanceUtils.DEG_270_AS_RADS, tmp, radius);
    double ll_lon = west[LONG] * DistanceUtils.RADIANS_TO_DEGREES;

    //TODO: can we reuse our bearing calculations?
    double angDist = DistanceUtils.angularDistance(distance, radius);//in radians

    // for the poles, do something slightly different - a polar "cap".
    // Also, note point[LAT] is in radians, but ur and ll are in degrees
    if (point[LAT] + angDist > DistanceUtils.DEG_90_AS_RADS) { // we cross the north pole
      ll_lat = Math.min(ll_lat, ur_lat);
      ur_lat = 90;
      ll_lon = -180;
      ur_lon = 180;
    } else if (point[LAT] - angDist < -DistanceUtils.DEG_90_AS_RADS) { // we cross the south pole
      ur_lat = Math.max(ll_lat, ur_lat);
      ll_lat = -90;
      ll_lon = -180;
      ur_lon = 180;
    }

    //(end of code from LatLonType.createSpatialQuery())
    if (ll_lon <= ur_lon) {
      enclosingBox1 = new Rectangle(ll_lon,ll_lat,ur_lon,ur_lat);
    } else {
      enclosingBox1 = new Rectangle(Math.max(ll_lon,ur_lon),ll_lat,180,ur_lat);
      enclosingBox2 = new Rectangle(-180,ll_lat,Math.min(ll_lon,ur_lon),ur_lat);
    }
  }

  @Override
  public boolean contains(Point2D p) {
    return calcDistanceToPoint(p) <= distance;
  }

  public double calcDistanceToPoint(Point2D p) {
    return DistanceUtils.haversine(Math.toRadians(point.getY()), Math.toRadians(point.getX()),
        Math.toRadians(p.getY()), Math.toRadians(p.getX()), radius);
  }

  @Override
  public double area() {
    throw new UnsupportedOperationException("TODO unimplemented");//TODO
  }

  @Override
  public Point2D centroid() {
    return point;
  }

  @Override
  public Rectangle boundingRectangle() {
    if (enclosingBox2 == null)
      return enclosingBox1;
    //wrap longitude around the world (note: both boxes have same latitudes)
    return new Rectangle(-180,enclosingBox1.getMinY(),180,enclosingBox1.getMaxY());
  }

  public Rectangle getEnclosingBox1() {
    return enclosingBox1;
  }

  public Rectangle getEnclosingBox2() {
    return enclosingBox2;
  }

  @Override
  public IntersectCase intersect(Rectangle r) {
    //do quick check against bounding box for OUTSIDE
    if (enclosingBox1.intersect(r) == IntersectCase.OUTSIDE) {
      if (enclosingBox2 == null || enclosingBox2.intersect(r) == IntersectCase.OUTSIDE)
        return IntersectCase.OUTSIDE;
    }

    //do quick check to see if all corners are within this circle for CONTAINS
    if (contains(r.getMinPoint()) && contains(r.getMaxPoint())
            && contains(r.getMaxXMinYPoint())
            && contains(r.getMinXMaxYPoint()))
      return IntersectCase.CONTAINS;
    return IntersectCase.INTERSECTS;//needn't actually intersect; this is a good guess
  }

  @Override
  public String toString() {
    return "PointDistanceShape{" + point + ", distance=" + distance + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PointDistanceGeom that = (PointDistanceGeom) o;

    if (Double.compare(that.distance, distance) != 0) return false;
    if (Double.compare(that.radius, radius) != 0) return false;
    if (point != null ? !point.equals(that.point) : that.point != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = point != null ? point.hashCode() : 0;
    temp = distance != +0.0d ? Double.doubleToLongBits(distance) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = radius != +0.0d ? Double.doubleToLongBits(radius) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  public double getRadius() {
    return radius;
  }
}
