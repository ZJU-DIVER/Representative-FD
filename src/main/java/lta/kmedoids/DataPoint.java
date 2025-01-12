package RepFD.kmedoids;

import RepFD.FDResult;

import java.util.ArrayList;
import java.util.BitSet;

public class DataPoint {
    private FDResult dimension;
    private Cluster cluster;
    private double euDt;

    public DataPoint(FDResult dimension) {
        this.dimension = dimension;
        this.cluster = null;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public double calJaccardDistance(FDResult other) {
        BitSet l = other.getLhs();
        BitSet lhs = (BitSet) l.clone();
        int rhs = other.getRhs();
        BitSet XX = this.getDimensioin().getLhs();
        lhs.and(XX);

        double sim = lhs.cardinality();
        sim = sim / (XX.cardinality() + l.cardinality() - sim);
        sim = sim * 2;
        if (this.getDimensioin().getRhs() == rhs) {
            sim += 1;
        }
        return 3 - sim;
    }

    public double calEuclideanDistanceSum() {
        double sum = 0.0;
        Cluster cluster = this.getCluster();
        ArrayList<DataPoint> dataPoints = cluster.getDataPoints();

        for (int i = 0; i < dataPoints.size(); i++) {
            double temp = this.calJaccardDistance(dataPoints.get(i).getDimensioin());
            sum = sum + temp;
        }
        return Math.sqrt(sum);
    }

    public double testEuclideanDistance(Medoid c) {
        double sum = 0.0;
        FDResult cDim = c.getDimensioin();
        double temp = this.calJaccardDistance(cDim);
        sum = sum + temp;
        return Math.sqrt(sum);
    }

    public FDResult getDimensioin() {
        return this.dimension;
    }

    public Cluster getCluster() {
        return this.cluster;
    }

    public double getCurrentEuDt() {
        return this.euDt;
    }
}



