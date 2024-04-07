package RepFD.kmedoids;

import RepFD.FDResult;

import java.util.*;

public class TestKmedoids {
    private int numberAttributes;
    public int newP;
    public ArrayList<DataPoint> dataPoints;
    public FDResult[] medoids;//中心点集，长度k
    public ClusterAnalysis ca;

    public TestKmedoids(int num, ArrayList<DataPoint> dataPoints) {
        this.newP = 0;
        this.numberAttributes = num;
        this.dataPoints = dataPoints;
    }

    public void addPoints(DataPoint e) {
        this.dataPoints.add(e);
        this.newP++;
    }

    public void reTest(int k) {//增量聚类
        this.ca.addNewP(this.dataPoints.subList(this.dataPoints.size() - newP, this.dataPoints.size()));
        this.newP = 0;
        this.ca.startAnalysis(k);
        this.medoids = this.ca.getMedoids();
    }

    public void test(int k) {//是第一次聚类
        Random random = new Random();
        FDResult[] med = new FDResult[k];

        for (int i = 0; i < k; i++) {
            int indexs = random.nextInt(this.dataPoints.size());
            med[i] = (this.dataPoints.get(indexs).getDimensioin());
        }
        this.ca = new ClusterAnalysis(k, 0, this.dataPoints, med);
        this.ca.startAnalysis(k);
        this.medoids = this.ca.getMedoids();
//        ArrayList<DataPoint>[] v = ca.getClusterOutput();
//        for (int ii=0; ii<v.length; ii++){
//            ArrayList tempV = v[ii];
//            System.out.println("-----------Cluster"+ii+"---------");
//            Iterator iter = tempV.iterator();
//            while(iter.hasNext()){
//                DataPoint dpTemp = (DataPoint)iter.next();
//                System.out.println(dpTemp.getPointName());
//            }
//        }
        this.newP = 0;
    }
}