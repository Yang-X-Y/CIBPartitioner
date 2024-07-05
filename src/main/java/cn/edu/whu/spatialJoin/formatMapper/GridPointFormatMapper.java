package cn.edu.whu.spatialJoin.formatMapper;

import cn.edu.whu.spatialJoin.JTS.GridGeometryTransformer;
import cn.edu.whu.spatialJoin.JTS.GridLineString;
import cn.edu.whu.spatialJoin.JTS.GridPoint;
import cn.edu.whu.spatialJoin.JTS.GridPolygon;
import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

public class GridPointFormatMapper extends FormatMapper {

    protected final int level;

    public GridPointFormatMapper(int level) {
        super(0, 1, FileDataSplitter.WKT, true, GeometryType.GRIDPOINT);
        this.level = level;
    }

    @Override
    public Geometry readGeometry(String line)
            throws ParseException {
        final String[] columns = line.split("\t");
        String geometryId = columns[0];
        Geometry geometry = wktReader.read(columns[1]);
        if (!geometry.isValid()) return null; // JTS doesn't support not valid objects.
        geometry = GridGeometryTransformer.transformPoint((Point) geometry, geometryId, level);
        return geometry;
    }
}
