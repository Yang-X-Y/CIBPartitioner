package cn.edu.whu.spatialJoin.spatialPartitioning.CIBP;

import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.PriorityQueue;
import java.util.Queue;

public class CIBPTree {

    private Envelope env; // 区域跨度

    private double cellLen; // 格网单元边长

    private GridHistogramCI gridHistogramCI;

    private Node root;


    public CIBPTree(GridHistogramCI gridHistogramCI){
        this.gridHistogramCI = gridHistogramCI;
        this.env = gridHistogramCI.getEnv();
        this.cellLen = gridHistogramCI.getCellLen();
        this.root = null;
    }

    public void constructTree(int maxLeaf){
        Queue<GridHistogramCI> queue = new PriorityQueue<>((h1, h2) -> {
            if (h2.getCostSum()>h1.getCostSum()) {
                return 1;
            }
            else {
                return -1;
            }
        });
        queue.offer(gridHistogramCI);
        int unsplit = 0;
        while (!queue.isEmpty() && queue.size()+unsplit < maxLeaf) {
            GridHistogramCI poll = queue.poll();
            int index = poll.split2();
            if (index == 0) {
                unsplit++;
                continue;
            }
            if (poll.getChildren()[0]!=null) { queue.offer(poll.getChildren()[0]); }
            if (poll.getChildren()[1]!=null) { queue.offer(poll.getChildren()[1]); }
        }
        this.root = createNode(gridHistogramCI);
    }

    private Node createNode(GridHistogramCI gridHistogramCI){
        double minX = env.getMinX() + gridHistogramCI.getWMin() * cellLen;
        double maxX = minX + (gridHistogramCI.getWMax() - gridHistogramCI.getWMin() + 1) * cellLen;
        double minY = env.getMinY() + gridHistogramCI.getHMin() * cellLen;
        double maxY = minY + (gridHistogramCI.getHMax() - gridHistogramCI.getHMin() + 1) * cellLen;
        Node node =  new Node(new Envelope(minX, maxX, minY, maxY), gridHistogramCI.getCostSum());
        if (gridHistogramCI.getChildren()[0] != null){
            node.children[0] = createNode(gridHistogramCI.getChildren()[0]);
        }
        if (gridHistogramCI.getChildren()[1] != null){
            node.children[1] = createNode(gridHistogramCI.getChildren()[1]);
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

    public GridHistogramCI getGridCostHistogram() {
        return gridHistogramCI;
    }
}
