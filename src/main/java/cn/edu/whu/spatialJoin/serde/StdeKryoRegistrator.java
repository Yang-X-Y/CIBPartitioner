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

package cn.edu.whu.spatialJoin.serde;

import cn.edu.whu.spatialJoin.geometryObjects.Circle;
import cn.edu.whu.spatialJoin.geometryObjects.GeometrySerde;
import cn.edu.whu.spatialJoin.geometryObjects.SpatialIndexSerde;
import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

public class StdeKryoRegistrator
        implements org.apache.spark.serializer.KryoRegistrator {

    final static Logger log = Logger.getLogger(StdeKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        GeometrySerde serializer = new GeometrySerde();
        SpatialIndexSerde indexSerializer = new SpatialIndexSerde(serializer);

        //log.info("Registering custom serializers for geometry types");

        kryo.register(Point.class, serializer);
        kryo.register(LineString.class, serializer);
        kryo.register(Polygon.class, serializer);
        kryo.register(MultiPoint.class, serializer);
        kryo.register(MultiLineString.class, serializer);
        kryo.register(MultiPolygon.class, serializer);
        kryo.register(GeometryCollection.class, serializer);
        kryo.register(Circle.class, serializer);
        kryo.register(Envelope.class, serializer);
        // TODO: Replace the default serializer with default spatial index serializer
        kryo.register(Quadtree.class, indexSerializer);
        kryo.register(STRtree.class, indexSerializer);
    }
}
