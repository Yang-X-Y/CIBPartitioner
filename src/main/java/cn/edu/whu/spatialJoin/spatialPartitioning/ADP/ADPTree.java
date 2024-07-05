package cn.edu.whu.spatialJoin.spatialPartitioning.ADP;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.PriorityQueue;
import java.util.Queue;

public class ADPTree {

    private Envelope env; // 区域跨度

    private double cellLen; // 格网单元边长

    private GridHistogramADP gridHistogramADP;

    private Node root;


    public ADPTree(GridHistogramADP gridHistogramADP, int cellNum){
        this.gridHistogramADP = gridHistogramADP;
        this.env = new Envelope(-180.0,180.0,-90.0,90.0);
        this.cellLen = Math.max(env.getHeight(), env.getWidth()) / cellNum;
        this.root = null;
    }

    public ADPTree(GridHistogramADP gridHistogramADP, int cellNum, Envelope envelope){
        this.gridHistogramADP = gridHistogramADP;
        this.env = envelope;
        this.cellLen = Math.max(env.getHeight(), env.getWidth()) / cellNum;
        this.root = null;
    }

    public void constructTree(int maxLeaf){
        Queue<GridHistogramADP> queue = new PriorityQueue<>((h1, h2) -> {
            if (h2.getCostSum()>h1.getCostSum()) {
                return 1;
            }
            else {
                return -1;
            }
        });
        queue.offer(gridHistogramADP);
        int unsplit = 0;
        while (!queue.isEmpty() && queue.size()+unsplit < maxLeaf) {
            GridHistogramADP poll = queue.poll();
//            GeometryFactory gf = new GeometryFactory();
//            System.out.println("range: "+Arrays.toString(poll.getRange())+"\tcostSum: "+poll.getCostSum()+"\t"+gf.toGeometry(poll.getEnvelope(env,cellLen)));
            int index = poll.split();
            if (index == 0) {
                unsplit++;
                continue;
            }
            queue.offer(poll.getChildren()[0]);
            queue.offer(poll.getChildren()[1]);
        }
        this.root = createNode(gridHistogramADP);
    }

    private Node createNode(GridHistogramADP gridHistogramADP){
        double minX = env.getMinX() + gridHistogramADP.getWMin() * cellLen;
        double maxX = minX + (gridHistogramADP.getWMax() - gridHistogramADP.getWMin() + 1) * cellLen;
        double minY = env.getMinY() + gridHistogramADP.getHMin() * cellLen;
        double maxY = minY + (gridHistogramADP.getHMax() - gridHistogramADP.getHMin() + 1) * cellLen;
        Node node =  new Node(new Envelope(minX, maxX, minY, maxY), gridHistogramADP.getCostSum());

        if (gridHistogramADP.getChildren()[0] != null){
            node.children[0] = createNode(gridHistogramADP.getChildren()[0]);
        }
        if (gridHistogramADP.getChildren()[1] != null){
            node.children[1] = createNode(gridHistogramADP.getChildren()[1]);
        }
        return node;
    }

    public static class Node implements Serializable {

        Envelope nodeEnv;

        double costSum;

        Node[] children;


        public Node(Envelope env, double costSum){
            this.nodeEnv = env;
            this.costSum = costSum;
            this.children = new Node[2];
        }

        public boolean isLeaf(){
            return children[0] == null && children[1] == null;
        }

        /**
         * Traverses the tree top-down breadth-first and calls the visitor
         * for each node. Stops traversing if a call to visit returns false.
         */
        public final void traverse(Visitor visitor)
        {
            if (!visitor.visit(this)) {
                return;
            }
            if (children != null) {
                for(Node child : children){
                    if (child != null) child.traverse(visitor);
                }
            }
        }

        public Envelope getNodeEnv() {
            return nodeEnv;
        }
    }


    public interface Visitor{
        boolean visit(Node node);
    }

    public Node getRoot() {
        return root;
    }

    public GridHistogramADP getGridCostHistogram() {
        return gridHistogramADP;
    }
}
