import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.apache.commons.math.util.MathUtils.EPSILON;

public class Main {
    private static boolean convergance(List<double[]> old_centroids, List<double[]> newCentroids) {
        if (old_centroids == null || newCentroids == null || old_centroids.size() == 0 || newCentroids.size() == 0) return false;
        for (int i = 0; i < old_centroids.size() && i < newCentroids.size(); i++) {
            double[] c1 = old_centroids.get(i);
            double[] c2 = newCentroids.get(i);
            for (int j = 0; j < c1.length - 1; j++) {
                if(Double.compare(c1[j], c2[j]) != 0){
                    return false;
                }
            }

        }
        return true;
    }


    private static List<double[]> update_old_cent(List<double[]> neww){
        List<double[]> old = new ArrayList<>();
        for(int i=0;i<neww.size();i++){
            old.add(new double[4]);
        }
        for (int i=0;i<neww.size();i++){
            for(int j=0;j<4;j++){
                old.get(i)[j] = neww.get(i)[j];
            }
        }
        return old;
    }


    public static void main(String[] args) throws IOException{
        System.out.println("Omar");
        if (args.length != 3) {
            System.out.println("Please enter 4 args as follow : <inputFilePath> <outputFilePath> <K for centroids>");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        int k = Integer.parseInt(args[2]);
        PKMeans kMeans = new PKMeans(input, output, k);
        List<double[]> old_centroids;
        List<double[]> new_centroids = new ArrayList<>();
        do{
            old_centroids = update_old_cent(new_centroids);
            new_centroids = kMeans.runKMeans(old_centroids);
        }while(!convergance(old_centroids, new_centroids));
    }
}
