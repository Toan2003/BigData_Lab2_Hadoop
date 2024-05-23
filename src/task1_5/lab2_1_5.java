import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap;
import org.apache.hadoop.fs.FileUtil;



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

public class lab2_1_5 {
  
  //termId to term namw
  public static HashMap<String, String> termNameOfId = new HashMap<>();

  public static void getTermName(String pathToTerms) {
    try (BufferedReader br = new BufferedReader(new FileReader(pathToTerms))) {
      String line;
      int rowNum = 1;
      while ((line = br.readLine()) != null) {
        termNameOfId.put(String.valueOf(rowNum), line);
        rowNum++;

      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  //class id
  public static HashMap<String, String> classIdOfDoc = new HashMap<>();

  public static void addClassId(String pathToClasses) {
    try (BufferedReader br = new BufferedReader(new FileReader(pathToClasses))) {
      String line;
      int lineSkip = 0;
      while ((line = br.readLine()) != null) {
        if (lineSkip < 4) {
          lineSkip++;
          continue;
        }
        String[] words = line.split(" ");
        int newDocId = Integer.parseInt(words[0])+1;
        classIdOfDoc.put(String.valueOf(newDocId), words[1]);

      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    public Integer row = 0;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (row <2) {
        row++;
        return;
      }
      String[] words = value.toString().split("\\s+");

      String term = words[0];
      String doc = words[1];
      double tfidf = Double.parseDouble(words[2]);
      // System.out.print(doc + " ");
      String classId = classIdOfDoc.get(doc);
      if (classId == null) {
        System.out.println("Class id not found for doc: " + doc);
        // return;
      }
      // System.out.print(classId + " ");
      context.write(new Text(classId + "-" + term), new Text(String.valueOf(tfidf)));
    }
  }

  public static class MyCombiner
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      double len = 0;
      double sum = 0;
      for (Text val : values) {
        double tfidf = Double.parseDouble(val.toString());
        sum += tfidf;
        len++;
      }
      double avg = sum/len;

      String[] parts = key.toString().split("-");
      String classId = parts[0];
      String term = parts[1];


      context.write(new Text(classId), new Text(term + "-"+avg));


    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      ArrayList<Map.Entry<String, Double>> term_avg = new ArrayList<>();

      for (Text val : values) {
        String[] temp = val.toString().split("-");
        term_avg.add(new AbstractMap.SimpleEntry<>(temp[0], Double.parseDouble(temp[1])));
      }

      Collections.sort(term_avg, Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));


      String out = "";
      int count = 0;
      for (Map.Entry<String, Double> entry : term_avg) {
        if (count >= 5)
          break;
        // context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        out += termNameOfId.get(entry.getKey()) + ":"+entry.getValue() + ", ";
        count++;
      }

      String className = key.toString();
      if (className.equals("0"))
        className = "Business";
      else if (className.equals("1"))
        className = "Entertainment";
      else if (className.equals("2"))
        className = "Politics";
      else if (className.equals("3"))
        className = "Sport";
      else if (className.equals("4"))
        className = "Tech";


      context.write(new Text(className), new Text(out));
    }
  }


  public static void main(String[] args) throws Exception {
    String pathToClasses = "../Input/bbc-preprocess/bbc.classes";
    addClassId(pathToClasses);
    getTermName("../Input/bbc-preprocess/bbc.terms");
    // System.out.println(termNameOfId);
    Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Lab2 lab2_1_5");
      job.setJarByClass(lab2_1_5.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setCombinerClass(MyCombiner.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.setInputDirRecursive(job, true);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      Path outputPath = new Path(args[1]);
      FileOutputFormat.setOutputPath(job, outputPath);

      // Run the job and wait for completion
    boolean success = job.waitForCompletion(true);

    // After job completion, copy part-r-00000 to local file system
    if (success) {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsFilePath = new Path(outputPath, "part-r-00000");
        Path localFilePath = new Path("./", "task_1_5.txt");
        fs.copyToLocalFile(false, hdfsFilePath, localFilePath);
        System.out.println("File copied successfully.");
        System.exit(0);
    } else {
        System.err.println("MapReduce job failed.");
        System.exit(1);
    }
  }
}