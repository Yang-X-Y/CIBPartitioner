package cn.edu.whu.spatialJoin.spatialPartitioning.CIBP;

import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.joinJudgement.DedupParams;
import cn.edu.whu.spatialJoin.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CIBPartitioner extends SpatialPartitioner {

    private CIBPTree.Node root;

    private Map<CIBPTree.Node, Integer> partitionIds;

    public CIBPartitioner(CIBPTree.Node root) {
        super(PartitionerType.GRIDKDBTREE, getLeafZones(root));
        this.root = root;
        this.partitionIds = new HashMap<>();
        collectPartitions();
    }

    private void collectPartitions(){
        AtomicInteger count = new AtomicInteger(0);
        root.traverse(new CIBPTree.Visitor() {
            @Override
            public boolean visit(CIBPTree.Node node) {
                if (node.isLeaf()){
                    partitionIds.put(node, count.getAndIncrement());
                }
                return true;
            }
        });
    }

    private static List<Envelope> getLeafZones(CIBPTree.Node root) {
        final List<Envelope> leafs = new ArrayList<>();
        GeometryFactory gf = new GeometryFactory();

        root.traverse(new CIBPTree.Visitor() {
            @Override
            public boolean visit(CIBPTree.Node node) {
                if (node.isLeaf()){
                    leafs.add(node.nodeEnv);
//                    System.out.println("costSum: "+node.costSum+" "+gf.toGeometry(node.nodeEnv));
                }
                return true;
            }
        });

        return leafs;
    }

    @Override
    public int numPartitions() {
        return partitionIds.size();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception {

        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        root.traverse(new CIBPTree.Visitor() {
            @Override
            public boolean visit(CIBPTree.Node node) {
                if (!node.nodeEnv.intersects(envelope)){
                    return false;
                }
                if (node.isLeaf()){
                    result.add(new Tuple2<>(partitionIds.get(node), spatialObject));
                }
                return true;
            }
        });
        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        Envelope[] envArr = new Envelope[partitionIds.size()];
        Double[] costArr = new Double[partitionIds.size()];
        for (Map.Entry<CIBPTree.Node, Integer> entry : partitionIds.entrySet()){
            envArr[entry.getValue()] = entry.getKey().nodeEnv;
            costArr[entry.getValue()] = entry.getKey().costSum;
        }
        return new DedupParams(Arrays.asList(envArr.clone()),Arrays.asList(costArr.clone()));
    }

    @Override
    public int getPartition(Object key) {
        return (int)key;
    }
}
