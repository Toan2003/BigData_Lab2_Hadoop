import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
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

public class task1_2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    public Integer row = 0;
    private Text word = new Text();
    private Text value1 = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (row < 2) {
        ++row;
        return;
      }
      String[] words = value.toString().split("\\s+");
      word.set(words[0]);
      value1.set(words[1]+" "+words[2]);
      context.write(word, value1);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      ArrayList<String> doc_freq = new ArrayList<String>();
      int count = 0;
      int sum = 0;
      for (Text val : values) {
        doc_freq.add(val.toString());
        String[]temp = doc_freq.get(count).split("\\s+");
        sum += Integer.parseInt(temp[1]);
        ++count;
      }
      if (sum >= 3) {
        Text out = new Text();
        for (int i = 0; i < count; ++i) {
          out.set(doc_freq.get(i));
          context.write(key, out);
        }
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
    addDocId("../Input/bbc-preprocess/bbc.docs");
    addTermId("../Input/bbc-preprocess/bbc.terms");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lab2 task1_2");
    job.setJarByClass(task1_2.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    if (job.waitForCompletion(true)) {
      CombineOutputFiles.outputMTXFiles(args[1]);
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}