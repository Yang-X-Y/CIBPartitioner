package cn.edu.whu.spatialJoin.formatMapper;

import cn.edu.whu.spatialJoin.JTS.GridGeometryTransformer;
import cn.edu.whu.spatialJoin.JTS.GridLineString;
import cn.edu.whu.spatialJoin.JTS.GridPoint;
import cn.edu.whu.spatialJoin.JTS.GridPolygon;
import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

public class GridLineStringFormatMapper extends FormatMapper {

    protected final int SEG;
    protected final boolean isFixedLevel;

    public GridLineStringFormatMapper(int SEG, boolean isFixedLevel) {
        super(0, -1, FileDataSplitter.WKT, true, GeometryType.GRIDLINESTRING);
        this.SEG = SEG;
        this.isFixedLevel = isFixedLevel;
    }

    @Override
    public Geometry readGeometry(String line)
            throws ParseException {
        final String[] columns = line.split("\t");
        String geometryId = columns[0];
        Geometry geometry = wktReader.read(columns[1]);
        if (!geometry.isValid()) return null; // JTS doesn't support not valid objects.
        if (isFixedLevel){
            geometry = new GridLineString(((LineString) geometry).getCoordinateSequence(), geometry.getFactory(),(byte) SEG);
        } else {
            geometry = new GridLineString(((LineString) geometry).getCoordinateSequence(), geometry.getFactory(),geometryId,SEG);
        }
        return geometry;
    }

}
