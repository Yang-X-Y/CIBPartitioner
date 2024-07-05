package cn.edu.whu.spatialJoin.formatMapper.shapefileParser.parseUtils.shp;

import cn.edu.whu.spatialJoin.JTS.Grid;
import cn.edu.whu.spatialJoin.JTS.GridPoint;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.IOException;

public class GridPointParser extends ShapeParser {

    /**
     * create a parser that can abstract a GridPoint from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public GridPointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract a GridPoint shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader) {
        double x = reader.readDouble();
        double y = reader.readDouble();
        byte level = reader.readByte();
        long id = reader.readLong();
        GridPoint point = new GridPoint(new CoordinateArraySequence(new Coordinate[]{new Coordinate(x, y)}), geometryFactory, new Grid(level, id));
        return point;
    }
}
