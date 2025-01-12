package RepFD.kmedoids;

import RepFD.FDResult;

import java.util.*;

public class ClusterAnalysis {
    private Cluster[] clusters;
    private int miter;
    private ArrayList<DataPoint> dataPoints = new ArrayList<DataPoint>();

    public ClusterAnalysis(int k, int iter, ArrayList<DataPoint> dataPoints, FDResult[] medoids) {
        clusters = new Cluster[k];
        for (int i = 0; i < k; i++) {
            clusters[i] = new Cluster("Cluster:" + i);
        }
        this.miter = iter;
        this.dataPoints = dataPoints;
        setInitialMedoids(medoids);
    }

    public void addNewP(List<DataPoint> newP) {
        List<DataPoint> ne = new ArrayList<>(newP);
        dataPoints.addAll(newP);
        for (DataPoint dataPoint : ne) {
            int clusterIndex = 0;
            double minDistance = Double.MAX_VALUE;

            for (int k = 0; k < clusters.length; k++) {
                double eucDistance = dataPoint.testEuclideanDistance(clusters[k].getMedoid());
                if (eucDistance < minDistance) {
                    minDistance = eucDistance;
                    clusterIndex = k;
                }
            }

            clusters[clusterIndex].addDataPoint(dataPoint);
        }

        for (int m = 0; m < clusters.length; m++) {
            clusters[m].getMedoid().calcMedoid();

        }
    }

    public FDResult[] getMedoids() {
        FDResult[] fdr = new FDResult[clusters.length];
        for (int i = 0; i < clusters.length; i++) {
            fdr[i] = clusters[i].getMedoid().getDimensioin();
        }
        return fdr;
    }

    public int getIterations() {
        return miter;
    }

    public ArrayList<DataPoint>[] getClusterOutput() {
        ArrayList<DataPoint>[] v = new ArrayList[clusters.length];
        for (int i = 0; i < clusters.length; i++) {
            v[i] = clusters[i].getDataPoints();
        }
        return v;
    }

    public void startAnalysis(int key) {
        //setInitialMedoids(medoids);
        FDResult[] newMedoids = this.getMedoids();
        FDResult[] oldMedoids = new FDResult[newMedoids.length];
        boolean ifStop = false;
        while (!isEqual(oldMedoids, newMedoids)) {
            ifStop = true;
            for (Cluster cluster : clusters) {
                cluster.getDataPoints().clear();
            }
            for (DataPoint dataPoint : dataPoints) {
                int clusterIndex = 0;
                double minDistance = 3;

                for (int k = 0; k < clusters.length; k++) {
                    double eucDistance = dataPoint.testEuclideanDistance(clusters[k].getMedoid());
                    if (eucDistance < minDistance) {
                        minDistance = eucDistance;
                        clusterIndex = k;
                    }
                }

                clusters[clusterIndex].addDataPoint(dataPoint);
            }

            for (int m = 0; m < clusters.length; m++) {
                clusters[m].getMedoid().calcMedoid();
            }

            for (int i = 0; i < newMedoids.length; i++) {
                oldMedoids[i] = newMedoids[i];
            }

            for (int n = 0; n < clusters.length; n++) {
                newMedoids[n] = clusters[n].getMedoid().getDimensioin();
            }
            this.miter++;
        }
    }

    private void setInitialMedoids(FDResult[] medoids) {
        for (int n = 0; n < clusters.length; n++) {
            Medoid medoid = new Medoid(medoids[n]);
            clusters[n].setMedoid(medoid);
            medoid.setCluster(clusters[n]);
        }
    }

    private boolean isEqual(FDResult[] oldMedoids, FDResult[] newMedoids) {
        boolean flag = false;
        for (int i = 0; i < oldMedoids.length; i++) {
            if (oldMedoids[i] != newMedoids[i]) {
                return flag;
            }
        }
        flag = true;
        return flag;
    }
}