package RepFD.kmedoids;

import RepFD.FDResult;

import java.util.ArrayList;

public class Medoid {
    private FDResult dimension; // 质点的维度
    private Cluster cluster; //所属类簇
    private double etdDisSum;//Medoid到本类簇中所有的欧式距离之和

    public Medoid(FDResult dimension) {
        this.dimension = dimension;
    }

    public void setCluster(Cluster c) {
        this.cluster = c;
    }

    public void calcMedoid() {// 取代价最小的点
        //boolean changed=false;
        calcEtdDisSum();
        double minEucDisSum = this.etdDisSum;
        ArrayList<DataPoint> dps = this.cluster.getDataPoints();
        for (int i = 0; i < dps.size(); i++) {
            double tempeucDisSum = dps.get(i).calEuclideanDistanceSum();
            if (tempeucDisSum < minEucDisSum) {
                dimension = dps.get(i).getDimensioin();
                minEucDisSum = tempeucDisSum;
                // changed=true;
            }
        }
        // return changed;
    }

    // 计算该Medoid到同类簇所有样本点的欧斯距离和
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