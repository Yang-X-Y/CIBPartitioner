package cn.edu.whu.spatialJoin.formatMapper.shapefileParser.parseUtils.shp;

import cn.edu.whu.spatialJoin.JTS.Grid;
import cn.edu.whu.spatialJoin.JTS.GridPolygon;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GridPolygonParser extends ShapeParser {
    /**
     * create a parser that can abstract a GridPolygon from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public GridPolygonParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract abstract a GridPolygon shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader) {
        reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);

        int numRings = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numRings, numPoints);

        boolean shellsCCW = false;

        LinearRing shell = null;
        List<LinearRing> holes = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();

        for (int i = 0; i < numRings; ++i) {
            int readScale = offsets[i + 1] - offsets[i];
            CoordinateSequence csRing = readCoordinates(reader, readScale);

            if (csRing.size() <= 3) {
                continue; // if points less than 3, it's not a ring, we just abandon it
            }

            LinearRing ring = geometryFactory.createLinearRing(csRing);
            if (shell == null) {
                shell = ring;
                shellsCCW = CoordinateSequences.isCCW(csRing);
            } else if (CoordinateSequences.isCCW(csRing) != shellsCCW) {
                holes.add(ring);
            } else {
                Polygon polygon = geometryFactory.createPolygon(shell, GeometryFactory.toLinearRingArray(holes));
                polygons.add(polygon);

                shell = ring;
                holes.clear();
            }
        }

        int numGrids = reader.readInt();
        Grid[] ids = new Grid[numGrids];

        for (int i = 0; i < numGrids; i++) {
            byte level = reader.readByte();
            Long id = reader.readLong();
            boolean contain = reader.readInt() == 1;
            ids[i] = new Grid(level, id, contain);
        }

        if (shell != null) {
            GridPolygon polygon = new GridPolygon(shell, null, geometryFactory, ids);
            polygons.add(polygon);
        }

        return polygons.get(0);
    }
}
