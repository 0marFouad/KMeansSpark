import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class PKMeans {

    private String inputFile;
    private String outputFile;
    private int numOfCentroids;
    private JavaSparkContext sc;
    private int iterations;

    PKMeans(String input, String output, int num) {
        this.inputFile = input;
        this.outputFile = output;
        this.numOfCentroids = num;
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KMeans");
        sc = new JavaSparkContext(conf);
        iterations = 0;
    }

    public List<double[]> runKMeans(List<double[]> input_centroids) throws IOException {

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up line to form a datapoint.
        JavaRDD<double[]> points = input.map(line -> {
            String[] strings = line.split(",");
            double[] point = IntStream.range(0, strings.length - 1).mapToDouble(i -> Double.parseDouble(strings[i])).toArray();
            double[] modified_point = new double[point.length+1];
            for (int i=0;i<point.length;i++){
                modified_point[i] = point[i];
            }
            modified_point[modified_point.length-1] = 1;
            return modified_point;
        });
        List<double[]> centroidsRead;
        if(iterations < 1){
            centroidsRead = points.takeSample(false,numOfCentroids);
        }else{
            centroidsRead = input_centroids;
        }



        List<Tuple2<Integer, double[]>> centroidsPairs = new ArrayList<Tuple2<Integer, double[]>>();
        int i = 0;
        for (double[] centroid : centroidsRead) {
            centroidsPairs.add(new Tuple2<>(i, centroid));
            i++;
        }
        JavaPairRDD<Integer, double[]> centroids = sc.<Integer, double[]>parallelizePairs(centroidsPairs);
        List<double[]> list = centroids.sortByKey().values().collect();

        JavaPairRDD<Integer, double[]> pointsPair = points.mapToPair(point -> {
            int index = Helper.getClosestCentroid(point, list);
            return new Tuple2<>(index, point);
        });




        JavaPairRDD<Integer, double[]> reducerCentroids = pointsPair.reduceByKey((point, sum) -> {
            double[] total = new double[point.length];
            total[total.length - 1] = point[point.length - 1] + sum[sum.length - 1];
            for (int j = 0; j < point.length - 1; j++) {
                total[j] = (point[j])*(point[point.length-1]) + sum[j]*(sum[sum.length-1]);
                total[j] /= total[total.length-1];
            }

            return total;
        });




        List<double[]> list1 = reducerCentroids.sortByKey().values().collect();
        System.out.println("Iteration # " + iterations);
        for(int ii=0;ii<list1.size();ii++){
            for(int j=0;j<list1.get(ii).length;j++){
                System.out.print(list1.get(ii)[j] + " ");
            }
            System.out.println();
        }
        System.out.println();

        //this.sc.close();
        iterations++;
        return list1;
    }

}