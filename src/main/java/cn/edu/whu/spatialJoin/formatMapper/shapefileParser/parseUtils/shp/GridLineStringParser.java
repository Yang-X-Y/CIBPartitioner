package cn.edu.whu.spatialJoin.formatMapper.shapefileParser.parseUtils.shp;

import cn.edu.whu.spatialJoin.JTS.GridLineString;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;

public class GridLineStringParser extends ShapeParser {

    /**
     * create a parser that can abstract a GridLineString from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public GridLineStringParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract a GridLineString shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader) {
       /* reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numParts, numPoints);

        GridLineString[] lines = new GridLineString[numParts];
        int readScale = offsets[1] - offsets[0];
        CoordinateSequence csString = readCoordinates(reader, readScale);

        int numsGrids = reader.readInt();
        Long[] ids = new Long[numsGrids];
        for(int i=0 ;i < numsGrids;i++){
            ids[i] = reader.readLong();
        }
        byte level = reader.readByte();
        lines[0] = new GridLineString(csString,geometryFactory,ids,level);*/

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
        GridLineString gridLineString = new GridLineString(coordinateSequences[0], geometryFactory, ids, level);

        return gridLineString;
    }
}
