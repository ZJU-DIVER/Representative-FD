package RepFD.kmedoids;

import RepFD.FDResult;

import java.util.ArrayList;

public class Medoid {
    private FDResult dimension;
    private Cluster cluster;
    private double etdDisSum;

    public Medoid(FDResult dimension) {
        this.dimension = dimension;
    }

    public void setCluster(Cluster c) {
        this.cluster = c;
    }

    public void calcMedoid() {
        calcEtdDisSum();
        double minEucDisSum = this.etdDisSum;
        ArrayList<DataPoint> dps = this.cluster.getDataPoints();
        for (int i = 0; i < dps.size(); i++) {
            double tempeucDisSum = dps.get(i).calEuclideanDistanceSum();
            if (tempeucDisSum < minEucDisSum) {
                dimension = dps.get(i).getDimensioin();
                minEucDisSum = tempeucDisSum;
            }
        }
    }

    private void calcEtdDisSum() {
        double sum = 0.0;
        Cluster cluster = this.getCluster();
        ArrayList<DataPoint> dataPoints = cluster.getDataPoints();
        DataPoint copyMe = new DataPoint(this.getDimensioin());

        for (int i = 0; i < dataPoints.size(); i++) {
            double temp = copyMe.calJaccardDistance(dataPoints.get(i).getDimensioin());
            sum = sum + temp;

        }
        etdDisSum = sum;
    }

    public FDResult getDimensioin() {
        return this.dimension;
    }

    public Cluster getCluster() {
        return this.cluster;
    }
}