import java.util.List;

public class Helper {


    public static int getClosestCentroid(double[] dataPoint, List<double[]> centroids) {
        double minDistance = Integer.MAX_VALUE;
        int minIndex = -1;

        for (int i = 0; i < centroids.size(); i++) {
            double distance = getEuclideanDistance(dataPoint, centroids.get(i));
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return minIndex;

    }

    public static double getEuclideanDistance(double[] dataPoint, double[] centroid) {

        double sum = 0;
        for (int i = 0; i < dataPoint.length && i < centroid.length; i++) {
            double num = dataPoint[i];
            sum += Math.pow((centroid[i] - num), 2);
        }
        return sum;
    }



}