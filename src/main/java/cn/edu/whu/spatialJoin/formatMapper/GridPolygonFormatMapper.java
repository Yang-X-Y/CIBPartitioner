package cn.edu.whu.spatialJoin.formatMapper;

import cn.edu.whu.spatialJoin.JTS.GridGeometryTransformer;
import cn.edu.whu.spatialJoin.JTS.GridLineString;
import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

public class GridPolygonFormatMapper extends FormatMapper {
    protected final int SEG;

    public GridPolygonFormatMapper(int SEG) {
        super(0, -1, FileDataSplitter.WKT, true, GeometryType.GRIDPOLYGON);
        this.SEG = SEG;
    }

    @Override
    public Geometry readGeometry(String line)
            throws ParseException {
        final String[] columns = line.split("\t");
        String geometryId = columns[0];
        Geometry geometry = wktReader.read(columns[1]);
        if (!geometry.isValid()) return null; // JTS doesn't support not valid objects.
        geometry = GridGeometryTransformer.transformPolygon((Polygon) geometry,geometryId,SEG);
        return geometry;
    }
}
