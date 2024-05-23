import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileReader;
import java.io.FileNotFoundException;

public class task1_4 {
  
  public static class TFMapper
       extends Mapper<Object, Text, Text, Text>{
    public Integer row = 0;
    private Text term_freq = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (row < 2) {
        ++row;
        return;
      }
      String[] words = value.toString().split("\\s+");
      //termid + frequency
      term_freq.set(words[0]+" "+words[2]);
      
      context.write(new Text(words[1]),  term_freq);
    }
  }

  public static class TFReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      ArrayList<String> doc_freq = new ArrayList<String>();

      for (Text val : values) {
        doc_freq.add(val.toString());
      }
      for (String val : doc_freq) {
       String[] temp = val.split("\\s+");
      //  System.out.println(val);
       int freq = Integer.parseInt(temp[1]);
       double tf = (double)freq/(double)doc_freq.size();
       //term + docid, freq + tf
        context.write(new Text(temp[0]+" "+key.toString()), new Text(freq+ " " + Double.toString(tf)));
      }
    }
  }

  //term + docid, freq + tf
  public static class TF_IDFMapper
       extends Mapper<Object, Text, Text, Text>{
    public Integer row = 0;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (row < 2) {
        ++row;
        return;
      }
      String[] words = value.toString().split("\\s+");
      // System.out.println(value.toString());
      //term, docid + freq + tf
      context.write(new Text(words[0]),  new Text(words[1]+" "+words[2]+" "+ words[3]));
    }
  }

  public static class TF_IDFReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      ArrayList<String> doc_freq_tf = new ArrayList<String>();
      for (Text val : values) {
        doc_freq_tf.add(val.toString());
      }
      //term, docid + freq + tf
      for (String val : doc_freq_tf) {
        String[] temp = val.split("\\s+");
        String term = key.toString();
        int docid = Integer.parseInt(temp[0]);
        int freq = Integer.parseInt(temp[1]);
        double tf = Double.parseDouble(temp[2]);
        double idf = Math.log(docId.size()/doc_freq_tf.size());
        double tfidf = tf*idf;
        //term, docid + tfidf
        context.write(new Text(term), new Text(docid+ " " + Double.toString(tfidf)));
      }
    }
  }

  public static class CombineOutputFiles {
    public static ArrayList<String> ouputMTX = new ArrayList<String>();
    public static void outputMTXFiles(String outputAddr) throws IOException {

        String outputFile = outputAddr + "/MTX/output.mtx";
        // Create configuration and file system instances
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(outputFile);
        // Path inputPath = new Path(outputAddr);
        // Create output file
        FSDataOutputStream out = fs.create(outputPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        try {
            // Write Matrix Market header
            //writer.write("%%MatrixMarket matrix coordinate real general\n");
            //writer.write("termid docid frequency\n");
            
            FileStatus[] statuses = fs.globStatus(new Path(outputAddr + "/part-r-*"));
            for (FileStatus status : statuses) {
                Path partFilePath = status.getPath();
                // Open input stream for the part file
                FSDataInputStream in = fs.open(partFilePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                try {
                    //read ouput part-r-* files
                    
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        ouputMTX.add(parts[0] + " " + parts[1] + " " + parts[2] + "\n");
                    }
                } finally {
                    // Close input stream
                    reader.close();
                    in.close();
                }
            }
            // Sort ouputMTX
            Collections.sort(ouputMTX);
            writer.write("%%MatrixMarket matrix coordinate real general\n");
            writer.write(termId.size() + " " + docId.size() + " " + ouputMTX.size() + "\n");
            // Write output to file
            for (String line : ouputMTX) {
              writer.write(line);
            }
            // Close output file
            writer.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close file system
            fs.close();
        } 
        System.out.println("Output MTX file has been created successfully.");
    }
  }

  public static class CombineOutputFiles1 {
    public static ArrayList<String> ouputMTX = new ArrayList<String>();
    public static void outputMTXFiles(String outputAddr) throws IOException {

        String outputFile = outputAddr + "/MTX/output.mtx";
        // Create configuration and file system instances
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(outputFile);
        // Path inputPath = new Path(outputAddr);
        // Create output file
        FSDataOutputStream out = fs.create(outputPath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        try {
            // Write Matrix Market header
            //writer.write("%%MatrixMarket matrix coordinate real general\n");
            //writer.write("termid docid frequency\n");
            
            FileStatus[] statuses = fs.globStatus(new Path(outputAddr + "/part-r-*"));
            for (FileStatus status : statuses) {
                Path partFilePath = status.getPath();
                // Open input stream for the part file
                FSDataInputStream in = fs.open(partFilePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                try {
                    //read ouput part-r-* files
                    
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        ouputMTX.add(parts[0] + " " + parts[1] + " " + parts[2] + " " +parts[3]+"\n");
                    }
                } finally {
                    // Close input stream
                    reader.close();
                    in.close();
                }
            }
            // Sort ouputMTX
            Collections.sort(ouputMTX);
            writer.write("%%MatrixMarket matrix coordinate real general\n");
            writer.write(termId.size() + " " + docId.size() + " " + ouputMTX.size() + "\n");
            // Write output to file
            for (String line : ouputMTX) {
              writer.write(line);
            }
            // Close output file
            writer.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close file system
            fs.close();
        } 
        System.out.println("Output MTX file has been created successfully.");
    }
  }
    //doc id
  public static HashMap<String, Integer> docId = new HashMap<>();
  public static int docIdCount = 1;
  public static int getDocId(String docName){
    if (docId.containsKey(docName)){
      return docId.get(docName);
    } else {
      docId.put(docName, docIdCount);
      docIdCount++;
      return docIdCount-1;
    }
  }
  public static void addDocId(String fileDocId){
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(fileDocId));
      String line;
      String [] words;
      while ((line = br.readLine()) != null) {
        words = line.split(" ");
        for (String word : words) {
          // System.out.print(word);
          getDocId(word);
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  //term id
  public static ArrayList<String> termId = new ArrayList<String>();
  public static void addTermId(String fileTermId){
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(fileTermId));
      String line;
      String [] words;
      while ((line = br.readLine()) != null) {
        words = line.split(" ");
        for (String word : words) {
          termId.add(word);
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    addTermId("../Input/bbc-preprocess/bbc.terms");
    addDocId("../Input/bbc-preprocess/bbc.docs");

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lab2 task1_4_1");
    job.setJarByClass(task1_4.class);
    job.setMapperClass(TFMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(TFReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/TF")); 
    if (job.waitForCompletion(true)) {
      CombineOutputFiles1.outputMTXFiles(args[1]+"/TF");
      System.out.println("TF file has been created successfully.");
      Configuration conf1 = new Configuration();
      Job job2 = Job.getInstance(conf1, "Lab2 task1_4_2");
      job2.setJarByClass(task1_4.class);
      job2.setMapperClass(TF_IDFMapper.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      // job.setCombinerClass(IntSumReducer.class);
      job2.setReducerClass(TF_IDFReducer.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job2, new Path(args[1]+"/TF/MTX"));
      FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/TF_IDF"));
       
      if (job2.waitForCompletion(true)) {
        CombineOutputFiles.outputMTXFiles(args[1]+"/TF_IDF");
        System.out.println("TF_IDF file has been created successfully.");
        System.exit(0);
      } else{
        System.exit(1);
      }
    } else {
      System.exit(1);
    }
  }
}