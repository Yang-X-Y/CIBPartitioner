package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.geomesa.curve.Z2SFC;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Grid extends Envelope implements Comparable, Serializable {

    byte level;//level<=256
    boolean contain = false;
    long gridID;
    private Polygon polygon;
    private Envelope envelope;
    private List<Integer[]> intervals;
    private String binaryStrIndex;

    public Grid(byte level, long gridID) {
        this.level = level;
        this.gridID = gridID;
    }

    public Grid(byte level, Coordinate point) {
        this.level = level;
        //System.out.println("level:"+level);
        Z2SFC z2 = new Z2SFC(this.level);
        this.gridID = z2.index(point.x, point.y, false);
    }

    public Grid(byte level, long gridID, boolean contain) {
        this.level = level;
        this.gridID = gridID;
        this.contain = contain;
    }

    public static void main(String[] args) {
        Grid a = new Grid((byte) 0, 0);
        Grid b = new Grid((byte) 2, new Coordinate(179, 80));
        for (Grid x : a.getChildren()) {
            System.out.println(x.toString());
        }
        System.out.println(b.toString());
        System.out.println(a.compareTo(b));

    }

    public Grid[] getChildren() {
        return new Grid[]{new Grid((byte) (level + 1), gridID << 2),
                new Grid((byte) (level + 1), (gridID << 2) | 1),
                new Grid((byte) (level + 1), (gridID << 2) | 2),
                new Grid((byte) (level + 1), (gridID << 2) | 3)};
    }

    public static Envelope computeEnvelope(Grid grid){
//        System.out.println("grid.level:"+grid.level);
        if (grid.level==0) return new Envelope(-180, 180, -90, 90);
        Z2SFC z2 = new Z2SFC(grid.level);
        Tuple2<Object, Object> centLonLat = z2.invert(grid.gridID);
        double minLon = (Double) centLonLat._1() - (180.0 / (1 << grid.level));
        double minLat = (Double) centLonLat._2() - (90.0 / (1 << grid.level));
        double maxLon = (Double) centLonLat._1() + (180.0 / (1 << grid.level));
        double maxLat = (Double) centLonLat._2() + (90.0 / (1 << grid.level));
        return new Envelope(minLon, maxLon, minLat, maxLat);
    }

    public Envelope getEnvelope() {
        if (envelope == null){
            envelope = computeEnvelope(this);
        }
        return envelope;
    }

    public Polygon toPolygon() {
        if (polygon == null){
            polygon = (Polygon) new GeometryFactory().toGeometry(getEnvelope());
        }
        return polygon;
    }

    public static String toBinaryStrIndex(long index, int level){
        char[] charArr = new char[level * 2];
        for(int i = 0; i < charArr.length; i += 2){
            int offset = level * 2 - i - 1;
            charArr[i] = (index & (1L << offset)) == 0 ? '0' : '1';
            charArr[i + 1] = (index & (1L << (offset - 1))) == 0 ? '0' : '1';
        }
        return new String(charArr);
    }

    //将index转换为二进制字符串
    public String getBinaryStrIndex(){
        if (binaryStrIndex == null){
            binaryStrIndex = toBinaryStrIndex(gridID, level);
        }

        return binaryStrIndex;
    }

    public void setIntervals(GridGeometry gridGeom) {
        double[] boundary = GridUtils.getGridBoundary(this);
        Envelope env = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
        GeometryEnvelopeFilter gef = new GeometryEnvelopeFilter(env);
        gef.initLineCrosser();
        gridGeom.apply(gef);
        this.intervals=gef.getMap();
    }

    public List<Integer[]> getIntervals(){
        return this.intervals;
    }

    public long getFather() {
        return gridID >> 2;
    }

    public long lowestOnBit() {
        return gridID & -gridID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Grid grid = (Grid) o;
        return level == grid.level &&
                gridID == grid.gridID;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, gridID);
    }

    @Override
    public String toString() {
        return "String="+binaryStrIndex+" level=" + level + "  zid=" + gridID;
    }

    @Override
    public int compareTo(Object obj) {
        Grid o = (Grid) obj;
        if (o.level == this.level) {
            return Long.compare(this.gridID, o.gridID);
        } else if (o.level > this.level) {
            return Long.compare(this.gridID, o.gridID >> (2 * (o.level - this.level)));
        } else {
            return Long.compare(this.gridID >> (2 * (this.level - o.level)), o.gridID);
        }

    }

    public byte getLevel() {
        return level;
    }

    public boolean isContainGrid() {
        return contain;
    }

    public long getGridID() {
        return gridID;
    }
}
