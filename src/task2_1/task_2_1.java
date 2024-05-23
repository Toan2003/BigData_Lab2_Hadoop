import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;

public class task_2_1 {

    public static class Point implements Writable {
        private double x;
        private double y;

        public Point() {
            this.x = 0;
            this.y = 0;
        }

        public Point(String x, String y) {
            this.x = Double.parseDouble(x);
            this.y = Double.parseDouble(y);
        }

        public Point(Double x, Double y) {
            this.x = x;
            this.y = y;
        }

        public Point(String[] coor) {
            this.x = Double.parseDouble(coor[0]);
            this.y = Double.parseDouble(coor[1]);
        }

        public Double getX() {
            return this.x;
        }

        public Double getY() {
            return this.y;
        }

        public Point copy() {
            Point ret = new Point();
            ret.x = this.x;
            ret.y = this.y;
            return ret;
        }

        public void sum(Point p) {
            this.x += p.x;
            this.y += p.y;
        }

        public void set(String[] coor) {
            this.x = Double.parseDouble(coor[0]);
            this.y = Double.parseDouble(coor[1]);
        }

        public void readFields(final DataInput in) throws IOException {
            this.x = in.readDouble();
            this.y = in.readDouble();
        }

        public void write(final DataOutput out) throws IOException {
            out.writeDouble(this.x);
            out.writeDouble(this.y);
        }

        @Override
        public String toString() {
            StringBuilder point = new StringBuilder();
            point.append(this.x);
            point.append(",");
            point.append(this.y);
            return point.toString();
        }

        public Double distance(Point p) {
            Double dist = Double.MIN_VALUE;
            for (int i = 0; i < 2; i++) {
                dist += Math.pow(Math.abs(this.x - p.x), 2);
            }
            dist = Math.sqrt(dist);
            return dist;
        }
    }

    private boolean stoppingCriterion(Point[] oldCentroids, Point[] newCentroids, Double threshold) {
        boolean check = true;
        for(int i = 0; i < oldCentroids.length; i++) {
            check = (oldCentroids[i].distance(newCentroids[i]) <= threshold);
            if (!check) {
                return false;
            }
        }
        return true;
    }

    private Point[] centroidsInit(Configuration conf, String pathString, int k, int dataSetSize) throws IOException {
    	Point[] points = new Point[k];
    	
        //Create a sorted list of positions without duplicates
        //Positions are the line index of the random selected centroids
        List<Integer> positions = new ArrayList<Integer>();
        Random random = new Random();
        int pos;
        while (positions.size() < k) {
            pos = random.nextInt(dataSetSize);
            if (!positions.contains(pos)) {
                positions.add(pos);
            }
        }
        Collections.sort(positions);
        
        //File reading utils
        Path path = new Path(pathString);
    	FileSystem hdfs = FileSystem.get(conf);
    	FSDataInputStream in = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        //Get centroids from the file
        int row = 0;
        int i = 0;
        int position;
        while (i < positions.size()) {
            position = positions.get(i);
            String point = br.readLine();
            if (row == position) {  
                String[] elements = point.split("\\s+");
                Point newPoint = new Point(elements[0], elements[1]);
                points[i] = newPoint.copy();  
                i++;
            }
            row++;
        }   
        br.close();
        
    	return points;
    } 

    private Point[] readCentroids(Configuration conf, int k, String pathString)
      throws IOException, FileNotFoundException {
        Point[] points = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));
        
        for (int i = 0; i < status.length; i++) {
            //Read the centroids from the hdfs
            if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String[] keyValueSplit = br.readLine().split("\t"); //Split line in K,V
                int centroidId = Integer.parseInt(keyValueSplit[0]);
                String[] point = keyValueSplit[1].split(",");
                points[centroidId] = new Point(point);
                br.close();
            }
        }
        //Delete temp directory
        hdfs.delete(new Path(pathString), true); 

    	return points;
    }

    private static void finalize(Configuration conf, Point[] centroids, String output, String input) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/task_2_1.clusters.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write clusters in a unique file
        for(int i = 0; i < centroids.length; i++) {
            br.write(centroids[i].getX() + " " + centroids[i].getY());
            br.newLine();
        }
        br.close();

        //File reading utils
        Path path = new Path(input);
    	FSDataInputStream in = hdfs.open(path);
        BufferedReader read = new BufferedReader(new InputStreamReader(in));
        dos = hdfs.create(new Path(output + "/task_2_1.classes.txt"), true);
        BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(dos));

        //Get points from the file
        List<Point> points = new ArrayList<>();
        while (true) {
            String point = read.readLine(); 
            if (point != null) {
                String[] elements = point.split("\\s+");
                Point newPoint = new Point(elements[0], elements[1]);

                // Write point and class to a unique file
                int classes = 0;
                Double minDist = Double.MAX_VALUE;
                for (int k = 0; k < centroids.length; k++) {
                    Double dist = newPoint.distance(centroids[k]);
                    if (minDist > dist) {
                        minDist = dist;
                        classes = k;
                    }
                }
                wr.write((classes + 1) + " " + newPoint.getX() + " " + newPoint.getY());
                wr.newLine();
            } else {
                break;
            }  
        }   

        read.close();
        wr.close();
        hdfs.close();
    }

    // Define Mapper, Combiner and Reducer

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Point> {

        private List<Point> centroids = new ArrayList<Point>();
        private Point point = new Point();
        private IntWritable centroid = new IntWritable();
    
        public void setup(Context context) throws IOException, InterruptedException {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
    
            for (int i = 0; i < k; i++) {
                String[] centroid = context.getConfiguration().getStrings("centroid." + i);
                centroids.add(new Point(centroid));
            }
        }
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Contruct the point
            String[] pointString = value.toString().split("\\s+");
            point.set(pointString);
            
            // Initialize variables
            Double minDist = Double.MAX_VALUE;
            Double distance = 0.0d;
            int nearest = -1;
    
            // Find the closest centroid
            for (int i = 0; i < centroids.size(); i++) {
                distance = point.distance(centroids.get(i));
                if (distance < minDist) {
                    nearest = i;
                    minDist = distance;
                }
            }

            centroid.set(nearest);
            context.write(centroid, point);
        }
    }

    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

        public void reduce(IntWritable centroid, Iterable<Point> points, Context context) throws InterruptedException, IOException {
            //Sum the points
            Point sum = new Point();
            for (Point point : points) {
                sum.sum(point);
            }
            
            context.write(centroid, sum);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {
        private final Text centroidId = new Text();
        private final Text centroidValue = new Text();
        
        public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context)
            throws IOException, InterruptedException {
            
            //Sum the partial sums
            int num_point = 0;
            Point sum = new Point();
            for (Point partSum : partialSums) {
                num_point += 1;
                sum.sum(partSum);
            }
    
            if (num_point != 0) {
                Double avgX = sum.getX() / num_point;
                Double avgY = sum.getY() / num_point;
                Point avg = new Point(avgX, avgY);
                
                centroidId.set(centroid.toString());
                centroidValue.set(avg.toString());
                context.write(centroidId, centroidValue);
            }
        }
    
    }

    public static void main(String[] args) throws Exception {
        task_2_1 object = new task_2_1();
        Configuration conf = new Configuration();

        final String INPUT = args[0];
        final String OUTPUT = args[1] + "/temp";
        final int DATASET_SIZE = 500;
        conf.setInt("dataset", DATASET_SIZE);
        final int K = 3;
        conf.setInt("k", K);
        final Double THRESHOLD = 0.0001d;
        conf.setDouble("threshold", THRESHOLD);
        final int MAX_ITERATIONS = 20;
        conf.setInt("max_iter", 20);

        Point[] oldCentroids = new Point[K];
        Point[] newCentroids = new Point[K];

        //Initial centroids
        newCentroids = object.centroidsInit(conf, INPUT, K, DATASET_SIZE);
        for (int i = 0; i < K; i++) {
            conf.set("centroid." + i, newCentroids[i].toString());
        }

        //MapReduce workflow
        boolean stop = false;
        boolean succeded = true;
        int i = 0;
        while (!stop) {
            i++;

            //Job configuration
            Job iteration = Job.getInstance(conf, "iter_" + i);
            iteration.setJarByClass(task_2_1.class);
            iteration.setMapperClass(KMeansMapper.class);
            iteration.setReducerClass(KMeansReducer.class);  
            iteration.setNumReduceTasks(K); //one task each centroid            
            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);
            FileInputFormat.addInputPath(iteration, new Path(INPUT));
            FileOutputFormat.setOutputPath(iteration, new Path(OUTPUT));
            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);

            boolean completed = iteration.waitForCompletion(true);

            //If the job fails the application will be closed.
            if (!completed) {        
                System.err.println("Iteration" + i + "failed.");
                System.exit(1);
            }

            if (completed) {
                //Save old centroids and read new centroids
                for (int id = 0; id < K; id++) {
                    oldCentroids[id] = newCentroids[id].copy();
                }                        
                newCentroids = object.readCentroids(conf, K, OUTPUT);

                //Check if centroids are changed
                stop = object.stoppingCriterion(oldCentroids, newCentroids, THRESHOLD);

                if (stop || i == (MAX_ITERATIONS)) {
                    finalize(conf, newCentroids, args[1], INPUT);
                    System.exit(0);
                } else {
                    //Set the new centroids in the configuration
                    for(int d = 0; d < K; d++) {
                        conf.unset("centroid." + d);
                        conf.set("centroid." + d, newCentroids[d].toString());
                    }
                }
            }

        }
    }

}
