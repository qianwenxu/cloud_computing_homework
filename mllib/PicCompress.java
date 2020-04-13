package spark_mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
 
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
 
public class PicCompress {
    public static void main(String[] args) throws IOException {
        BufferedImage bi = ImageIO.read(new File("picture/2.jpg"));
        HashMap<int[], Vector> rgbs = new HashMap<>();
        for (int x = 0; x < bi.getWidth(); x++) {
            for (int y = 0; y < bi.getHeight(); y++) {
                int[] pixel = bi.getRaster().getPixel(x, y, new int[3]);
                int[] point = new int[]{x, y};
                int r = pixel[0];
                int g = pixel[1];
                int b = pixel[2];
                //key为像素坐标, r,g,b特征构建密集矩阵
                rgbs.put(point, Vectors.dense((double) r, (double) g, (double) b));
            }
        }
        SparkConf conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        RDD<Vector> data = sc.parallelize(new ArrayList<>(rgbs.values())).rdd();
        data.cache();
 

        int k = 16;
        int runs = 10;
        int maxIterations = 20;
        KMeansModel model = KMeans.train(data, k, maxIterations, runs);
        
        ArrayList<double[]> cluster = new ArrayList<>();
        for (Vector c : model.clusterCenters()) {
            cluster.add(c.toArray());
        }
        HashMap<int[], double[]> newRgbs = new HashMap<>();
        
        for (Map.Entry rgb : rgbs.entrySet()) {
            int clusterKey = model.predict((Vector) rgb.getValue());
            newRgbs.put((int[]) rgb.getKey(), cluster.get(clusterKey));
        }
        
        BufferedImage image = new BufferedImage(bi.getWidth(), bi.getHeight(), BufferedImage.TYPE_INT_RGB);
        
        for (Map.Entry rgb : newRgbs.entrySet()) {
            int[] point = (int[]) rgb.getKey();
            double[] vRGB = (double[]) rgb.getValue();
            int RGB = ((int) vRGB[0] << 16) | ((int) vRGB[1] << 8) | ((int) vRGB[2]);
            image.setRGB(point[0], point[1], RGB);
        }
        
        ImageIO.write(image, "jpg", new File("picture/" + k + "_" + runs + "_2pic.jpg"));
        sc.stop();
    }
}