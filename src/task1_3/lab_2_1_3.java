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

public class lab_2_1_3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] words = value.toString().split("\\s+");
      int frequency = Integer.parseInt(words[2]);  
      context.write(new Text(words[0]), new Text(String.valueOf(frequency)));
    }
  }

  public static class MyCombiner
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      int sum = 0;
      for (Text val : values) {
        sum += Integer.parseInt(val.toString());
      }
      String out = key.toString() + "-" + sum;
      context.write(new Text("same"), new Text(out));


    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      ArrayList<Map.Entry<String, Integer>> term_freq = new ArrayList<>();

      for (Text val : values) {
        String[] temp = val.toString().split("-");
        term_freq.add(new AbstractMap.SimpleEntry<>(temp[0], Integer.parseInt(temp[1])));
      }

      Collections.sort(term_freq, Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));

      int count = 0;
      for (Map.Entry<String, Integer> entry : term_freq) {
        if (count >= 10)
          break;
        context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        count++;
      }
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Lab2 lab_2_1_3");
      job.setJarByClass(lab_2_1_3.class);
      job.setMapperClass(TokenizerMapper.class);
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
        Path localFilePath = new Path("./", "task_1_3.txt");
        fs.copyToLocalFile(false, hdfsFilePath, localFilePath);
        System.out.println("File copied successfully.");
        System.exit(0);
    } else {
        System.err.println("MapReduce job failed.");
        System.exit(1);
    }
  }
}