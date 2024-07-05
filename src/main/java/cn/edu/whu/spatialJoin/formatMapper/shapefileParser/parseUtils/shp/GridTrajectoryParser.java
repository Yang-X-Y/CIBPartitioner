package cn.edu.whu.spatialJoin.formatMapper.shapefileParser.parseUtils.shp;

import cn.edu.whu.spatialJoin.JTS.GridTrajectory;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class GridTrajectoryParser extends ShapeParser {

    public GridTrajectoryParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    @Override
    public Geometry parseShape(ShapeReader reader) {
        reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numParts, numPoints);

        CoordinateSequence[] coordinateSequences = new CoordinateSequence[numParts];
        for (int i = 0; i < numParts; ++i) {
            int readScale = offsets[i + 1] - offsets[i];
            CoordinateSequence csString = readCoordinates(reader, readScale);
            coordinateSequences[i] = csString;
        }

        int numsGrids = reader.readInt();
        Long[] ids = new Long[numsGrids];
        for (int i = 0; i < numsGrids; i++) {
            ids[i] = reader.readLong();
        }
        byte level = reader.readByte();

        if (numParts == 1) {
            return new GridTrajectory(coordinateSequences[0], geometryFactory, ids, level);
        } else {
            throw new RuntimeException("the numParts must be 1");
        }
    }

    @Override
    protected CoordinateSequence readCoordinates(ShapeReader reader, int numPoints) {

        CoordinateSequence coordinateSequence = geometryFactory.getCoordinateSequenceFactory().create
                (numPoints, 3, 0);
        for (int i = 0; i < numPoints; ++i) {
            coordinateSequence.setOrdinate(i, 0, reader.readDouble());
            coordinateSequence.setOrdinate(i, 1, reader.readDouble());
            coordinateSequence.setOrdinate(i, 2, reader.readDouble());
        }
        return coordinateSequence;
    }
}
