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

package cn.edu.whu.spatialJoin.formatMapper;

import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

public class FormatMapper<T extends Geometry>
        implements Serializable, FlatMapFunction<Iterator<String>, T> {
    final static Logger logger = Logger.getLogger(FormatMapper.class);
    /**
     * The start offset.
     */
    protected final int startOffset;
    /**
     * The end offset.
     */
    /* If the initial value is negative, SQL will consider each field as a spatial attribute if the target object is LineString or Polygon. */
    protected final int endOffset;
    /**
     * The splitter.
     */
    protected final FileDataSplitter splitter;
    /**
     * The carry input data.
     */
    protected final boolean carryInputData;
    /**
     * Non-spatial attributes in each input row will be concatenated to a tab separated string
     */
    protected String otherAttributes = "";
    protected GeometryType geometryType = null;
    /**
     * The factory.
     */
    transient protected GeometryFactory factory = new GeometryFactory();
    transient protected GeoJsonReader geoJSONReader = new GeoJsonReader();
    transient protected WKTReader wktReader = new WKTReader();
    /**
     * Allow mapping of invalid geometries.
     */
    boolean allowTopologicallyInvalidGeometries;
    // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt reader.
    /**
     * Crash on syntactically invalid geometries or skip them.
     */
    boolean skipSyntacticallyInvalidGeometries;

    /**
     * Instantiates a new format mapper.
     *
     * @param startOffset    the start offset
     * @param endOffset      the end offset
     * @param splitter       the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(int startOffset, int endOffset, FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.splitter = splitter;
        this.carryInputData = carryInputData;
        this.geometryType = geometryType;
        this.allowTopologicallyInvalidGeometries = true;
        this.skipSyntacticallyInvalidGeometries = false;
        // Only the following formats are allowed to use this format mapper because each input has the geometry type definition
        assert geometryType != null || splitter == FileDataSplitter.WKB || splitter == FileDataSplitter.WKT || splitter == FileDataSplitter.GEOJSON;
    }

    /**
     * Instantiates a new format mapper. This is extensively used in SQL.
     *
     * @param splitter
     * @param carryInputData
     */
    public FormatMapper(FileDataSplitter splitter, boolean carryInputData) {
        this(0, -1, splitter, carryInputData, null);
    }

    /**
     * This format mapper is used in SQL.
     *
     * @param splitter
     * @param carryInputData
     * @param geometryType
     */
    public FormatMapper(FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType) {
        this(0, -1, splitter, carryInputData, geometryType);
    }

    public static List<String> readGeoJsonPropertyNames(String geoJson) {
        if (geoJson.contains("Feature") || geoJson.contains("feature") || geoJson.contains("FEATURE")) {
            if (geoJson.contains("properties")) {
                Feature feature = (Feature) GeoJSONFactory.create(geoJson);
                if (Objects.isNull(feature.getId())) {
                    return new ArrayList(feature.getProperties().keySet());
                } else {
                    List<String> propertyList = new ArrayList<>(Arrays.asList("id"));
                    for (String geoJsonProperty : feature.getProperties().keySet()) {
                        propertyList.add(geoJsonProperty);
                    }
                    return propertyList;
                }
            }
        }
        logger.warn("[SQL] The GeoJSON file doesn't have feature properties");
        return null;
    }

    private void readObject(ObjectInputStream inputStream)
            throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        factory = new GeometryFactory();
        wktReader = new WKTReader();
        geoJSONReader = new GeoJsonReader();
    }

    public List<String> readPropertyNames(String geoString) {
        switch (splitter) {
            case GEOJSON:
                return readGeoJsonPropertyNames(geoString);
            default:
                return null;
        }
    }


    public <T extends Geometry> void addMultiGeometry(GeometryCollection multiGeometry, List<T> result) {
        for (int i = 0; i < multiGeometry.getNumGeometries(); i++) {
            T geometry = (T) multiGeometry.getGeometryN(i);
            geometry.setUserData(multiGeometry.getUserData());
            result.add(geometry);
        }
    }

    public Geometry readGeometry(String line)
            throws ParseException {
        final String[] columns = line.split("\t");
        String geometryId = columns[0];
        Geometry geometry = wktReader.read(columns[1]);
        if (!geometry.isValid()) return null; // JTS doesn't support not valid objects.

        return geometry;
    }

    @Override
    public Iterator<T> call(Iterator<String> stringIterator)
            throws Exception {
        List<T> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            addGeometry(readGeometry(line), result);
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<T> result) {
        if (geometry == null) {
            return;
        }
        if (geometry instanceof MultiPoint) {
            addMultiGeometry((MultiPoint) geometry, result);
        } else if (geometry instanceof MultiLineString) {
            addMultiGeometry((MultiLineString) geometry, result);
        } else if (geometry instanceof MultiPolygon) {
            addMultiGeometry((MultiPolygon) geometry, result);
        } else {
            result.add((T) geometry);
        }
    }
}
