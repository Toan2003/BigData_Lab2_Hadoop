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

public class task_2_2 {
    public static class Point implements Writable {
        private Text docID = new Text();
        private double[] tfidf = null;
        private int[] termid = null;

        public Point() {
            this.docID = new Text();
            this.tfidf = null;
            this.termid = null;
        }

        public Point copy() {
            return new Point(new Text(docID), tfidf.clone(), termid.clone());
        }

        public Point(String[] parts, int k) {
            this.docID = new Text(parts[0]);
            this.tfidf = new double[k];
            this.termid = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                String[] temp = parts[i].split(":");
                int tempid = Integer.parseInt(temp[0]);
                this.tfidf[tempid] = Double.parseDouble(temp[1]);
                this.termid[0] = Integer.parseInt(temp[0]);
            }
        }


        public Point(Text docID, double[] tfidf, int[] termid) {
            this.docID = docID;
            this.tfidf = new double[tfidf.length];
            for (int i = 0; i < tfidf.length; i++) {
                this.tfidf[i] = tfidf[i];
            }
            this.termid = new int[termid.length];
            for (int i = 0; i < termid.length; i++) {
                this.termid[i] = termid[i];
            }
        }

        public void setTfidf(double[] tfidf) {
            this.tfidf = new double[tfidf.length];
            for (int i = 0; i < tfidf.length; i++) {
                this.tfidf[i] = tfidf[i];
            }
        }

        public int[] setTermsId(int[] termid) {
            return this.termid = termid;
        }

        public Text getDocID() {
            return docID;
        }

        public double[] getTfidf() {
            return tfidf;
        }

        public int[] getTermsId() {
            return termid;
        }

        public int getNumTerms() {
            return termid.length;
        }

        public void sum(Point p){
            for (int i = 0; i < tfidf.length; i++) {
                tfidf[i] += p.tfidf[i];
            }
        }

        public void set(Text docID, double[] tfidf, int[] termid) {
            this.docID = docID;
            this.tfidf = tfidf;
            this.termid = termid;
        }

        public void readFields(DataInput in) throws IOException {
            docID.readFields(in);
            int numTerms = in.readInt();
            tfidf = new double[numTerms];
            termid = new int[numTerms];
            for (int i = 0; i < numTerms; i++) {
                tfidf[i] = in.readDouble();
                termid[i] = in.readInt();
            }
        }

        public void write(DataOutput out) throws IOException {
            docID.write(out);
            out.writeInt(termid.length);
            for (int i = 0; i < termid.length; i++) {
                // out.writeDouble(tfidf[i]);
                out.writeInt(termid[i]);
            }
        }

        @Override
        public String toString() {
            String PointToString = this.docID.toString() + "|";
            for (int i = 0; i < this.tfidf.length; i++) {
                String addedString = i + ":" + this.tfidf[i];
                PointToString += addedString;
                if (i < this.tfidf.length - 1) {
                    PointToString += ",";
                }
            }
            return PointToString;
        }

        public double cosineSimilarity(Point p) {
            double dotProduct = 0.0;
            double normA = 0.0;
            double normB = 0.0;
            for (int i = 0; i < tfidf.length; i++) {
                // System.out.println("centroid: " + tfidf);
                // System.out.println("point: " + p.tfidf);
                dotProduct += tfidf[i] * p.tfidf[i];
                normA += tfidf[i] * tfidf[i];
                normB += p.tfidf[i] * p.tfidf[i];
            }
            return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        }
    }

    private boolean stoppingCriterion(Point[] oldCenters, Point[] newCenters, double threshold) {
        for (int i = 0; i < oldCenters.length; i++) {
            if (oldCenters[i].cosineSimilarity(newCenters[i]) <= threshold) {
                return false;
            }
        }
        return true;
    }

    private Point[] centroidsInit(Configuration conf, String pathString, int k, int DATASET_SIZE, int totalTerms) throws IOException {
        Path path = new Path(pathString);
    	FileSystem hdfs = FileSystem.get(conf);
    	FSDataInputStream in = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        Point[] centers = new Point[k];
    
        // Initialize tfidf array outside the loop
        double[] tfidf = new double[totalTerms];
    
        Set<Integer> selectedDocs = new HashSet<>(); // To ensure unique document selection

        List<Integer> positions = new ArrayList<Integer>();
        Random random = new Random();
        int pos;
        while (positions.size() < k) {
            pos = random.nextInt(DATASET_SIZE);
            if (!positions.contains(pos)) {
                positions.add(pos);
            }
        }
        Collections.sort(positions);

        // Reset tfidf array for each centroid
        for (int j = 0; j < totalTerms; j++) {
            tfidf[j] = 0.0;
        }
        
        int i = 0;
        int position = 0;
        int row = 0;
        while (i < positions.size()) {
            position = positions.get(i);
            String line = br.readLine();
            if (row == position) {  
                String[] parts = line.split("\\|");
                String docID = parts[0];
                String[] terms = parts[1].split(",");
                int[] termid = new int[terms.length];

                for (int j = 0; j < terms.length; j++) {
                    int tempId = Integer.parseInt(terms[j].split(":")[0]);
                    double tempValue = Double.parseDouble(terms[j].split(":")[1]);
                    tfidf[tempId - 1] = tempValue;
                    termid[j] = tempId - 1;
                }

                centers[i] = new Point(new Text(docID), tfidf, termid);

                i++;
            }
            row++;
        }   
        br.close();

        return centers;
    }
    

    private Point[] readCentroids(Configuration conf, int k, String pathString) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString);
        FileStatus[] status = fs.listStatus(path);
        Point[] centers = new Point[k];
        for (int i = 0; i < k; i++) {
            Path file = status[i].getPath();
            FSDataInputStream in = fs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = br.readLine();
            String[] parts = line.split("\\|");
            String docID = parts[0];
            String[] terms = parts[1].split(",");
            int[] termid = new int[terms.length];
            double[] tfidf = new double[terms.length];
            for (int j = 0; j < terms.length; j++) {
                int tempId = Integer.parseInt(terms[j].split(":")[0]);
                double tempValue = Double.parseDouble(terms[j].split(":")[1]);
                tfidf[tempId-1] = tempValue;
                termid[j] = tempId-1;
            }
            centers[i] = new Point(new Text(docID), tfidf, termid);
            br.close();
            in.close();
        }
        return centers;
    }

    public static void finalize_kmeans(Configuration conf, Point[] centroids, String output, String input) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(output + "/final");
        FSDataOutputStream out = fs.create(path);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        for (int i = 0; i < centroids.length; i++) {
            bw.write(centroids[i].getDocID().toString() + "\t" + centroids[i].getTfidf().toString() + "\n");
        }
        bw.close();
        out.close();
    }
    
    public static class task_2_2_mapper extends Mapper<Object, Text, IntWritable, Point> {
        private List<Point> centroids = new ArrayList<Point>();
        private Point point = new Point();
        private IntWritable centroid = new IntWritable();

        public void setup(Context context) throws IOException, InterruptedException {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            int total_terms = Integer.parseInt(context.getConfiguration().get("total_terms"));

            for (int i = 0; i < k; i++){
                String[] centroid_init = context.getConfiguration().getStrings("centroid " + i);
                String docID = "";
                int[] termid = new int[centroid_init.length];
                double[] tfidf = new double[total_terms];
                for (int j = 0; j < total_terms; j++) {
                    tfidf[j] = 0.0;
                } 

                for (int j = 0; j < centroid_init.length; j++) {
                    if (j == 0) {
                        String[] element = centroid_init[j].split("\\|");
                        docID = element[0];
                        int tempId = Integer.parseInt(element[1].split(":")[0]);
                        termid[j] = tempId;
                        tfidf[0] = Double.parseDouble(element[1].split(":")[1]);
                    } else {
                        int tempId = Integer.parseInt(centroid_init[1].split(":")[0]);
                        termid[j] = tempId;
                        tfidf[j] = Double.parseDouble(centroid_init[j].split(":")[1]);
                    }
                    // System.out.println(centroid_init[j]);
                    // System.out.println("---------");
                }

                // String[] parts = centroid_init.split("\\|");
                // String docID = parts[0];
                // String[] terms = parts[1].split(",");
                // System.out.println("TERM " + terms.length);
                // int[] termid = new int[terms.length];
                // double[] tfidf = new double[total_terms];
            
                // for (int j = 0; j < total_terms; j++) {
                //     tfidf[j] = 0.0;
                // }

                // for(int j = 0; j < terms.length; j++) {
                //     int tempId = Integer.parseInt(terms[j].split(":")[0]);
                //     double tempValue = Double.parseDouble(terms[j].split(":")[1]);
                //     termid[j] = tempId;
                //     tfidf[tempId] = tempValue;
                // }

                // for (int l = 0; l < tfidf.length; l++) {
                //     System.out.print(tfidf[l] + " ");
                // }

                Point a = new Point(new Text(docID), tfidf, termid);
                System.out.println(a.toString());
                centroids.add(a);

            }

        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            int total_terms = Integer.parseInt(context.getConfiguration().get("total_terms"));            
            String[] parts = value.toString().split("\\|");
            String docID = parts[0];
            String[] terms = parts[1].split(",");
            // System.out.println(terms);
            int[] termid = new int[terms.length];
            
            double[] tfidf = new double[total_terms];
            
            for (int i = 0; i < total_terms; i++) {
                tfidf[i] = 0.0;
            }

            for(int i = 0; i < terms.length; i++) {
                int tempId = Integer.parseInt(terms[i].split(":")[0]);
                double tempValue = Double.parseDouble(terms[i].split(":")[1]);
                termid[i] = tempId;
                tfidf[tempId-1] = tempValue;
            }

            point.set(new Text(docID), tfidf, termid);

            double maxSimilarity = Double.MIN_VALUE;
            int cluster = 0;
            for (int i = 0; i < centroids.size(); i++) {
                // System.out.println("CENTROID " + centroids.get(i) );
                double similarity = point.cosineSimilarity(centroids.get(i));
                // System.out.println(similarity + " ");
                if (similarity > maxSimilarity) {
                    maxSimilarity = similarity;
                    cluster = i;
                }
            }
            // System.out.println();

            System.out.println("MAPPER " + cluster + " " + point.toString());
            centroid.set(cluster);
            context.write(centroid, point);
        }
    }
    public static class task_2_2_reducer extends Reducer<IntWritable, Point, Text, Text> {
        private final Text centroidId = new Text();
        private final Text centroidValue = new Text();
            
        public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
                
            System.out.println("-----------------------------------");

            Point newCenter = new Point();
            int count = 0;
            int total_terms = Integer.parseInt(context.getConfiguration().get("total_terms"));
            double[] setupTfidf = new double[total_terms];
            for (int i = 0; i < total_terms; i++) {
                setupTfidf[i] = 0.0;
            } 
            newCenter.setTfidf(setupTfidf);

            for (Point point : partialSums) {
                count++;
                newCenter.sum(point);
            }

            double[] tfidf = newCenter.getTfidf();
            for (int i = 0; i < tfidf.length; i++) {
                tfidf[i] = tfidf[i] / count;
            }
            newCenter.setTfidf(tfidf);
                
            centroidId.set(new Text(centroid.toString()));
            centroidValue.set(new Text(newCenter.toString()));
            context.write(centroidId, centroidValue);
        }
    }

    public static void main(String[] args) throws Exception {
        task_2_2 object = new task_2_2();
        Configuration conf = new Configuration();

        final String INPUT =  args[0];
        final String OUTPUT = args[1];
        final int DATASET_SIZE = 50;
        conf.setInt("dataset", DATASET_SIZE);
        final int K = 3;
        conf.setInt("k", K);
        final Double THRESHOLD = 0.0001d;
        conf.setDouble("threshold", THRESHOLD);
        final int MAX_ITERATIONS = 10;
        conf.setInt("max_iter", MAX_ITERATIONS);
        final int TOTAL_TERMS = 8;
        conf.setInt("total_terms", TOTAL_TERMS);

        Point[] oldCentroids = new Point[K];
        Point[] newCentroids = new Point[K];

        //Initalize centroids
        newCentroids = object.centroidsInit(conf, INPUT, K, DATASET_SIZE, TOTAL_TERMS);
        for (int i = 0; i < K; i++) {
            conf.set("centroid " + i, newCentroids[i].toString());
            System.out.println(newCentroids[i].toString());
        }

        //MapReduce workflow
        boolean stop = false;
        boolean succeded = true;
        int i = 0;
        while(!stop){
            i++;

            //Job Configuration
            Job iteration = Job.getInstance(conf, "iter_" + i);
            iteration.setJarByClass(task_2_2.class);
            iteration.setMapperClass(task_2_2_mapper.class);
            iteration.setReducerClass(task_2_2_reducer.class);
            // iteration.setNumReduceTasks(K);
            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);
            FileInputFormat.addInputPath(iteration, new Path(INPUT));
            FileOutputFormat.setOutputPath(iteration, new Path(OUTPUT + i));
            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);
            
            boolean completed = iteration.waitForCompletion(true);
            if(!completed){
                succeded = false;
                System.err.println("Iteration" + i + "failed.");
                break;
            }

            if(completed){
                stop = true;

                for(int j = 0; j < K; j++){
                    oldCentroids[j] = newCentroids[j].copy();
                }
                newCentroids = object.readCentroids(conf, K, OUTPUT + i);
                stop = object.stoppingCriterion(oldCentroids, newCentroids, THRESHOLD);

                if (stop || i == (MAX_ITERATIONS)) {
                    finalize_kmeans(conf, newCentroids, args[1], INPUT);
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
            
    };
}
