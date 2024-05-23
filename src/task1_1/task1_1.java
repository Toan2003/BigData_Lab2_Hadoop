import java.io.IOException;
import java.io.FileNotFoundException;

import java.io.DataOutputStream;
import java.util.StringTokenizer;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// read file
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.io.FileReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class task1_1 {
  // public static int numBerOfWord = 0;
  // public static int notStopWord = 0;
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public static String regEx = "^[.,()'\"-]+|[.,()'\"-]+$";
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      context.getInputSplit();
      Path filePath = ((FileSplit) context.getInputSplit()).getPath();
      String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();

      String[] fileArray = filePathString.split("/");
      int n = fileArray.length;
      String fileId = fileArray[n-2]+"."+fileArray[n-1].split("\\.")[0];
      // String fileId = fileArray[n-2]+"."+fileArray[n-1];
      StringTokenizer itr = new StringTokenizer(value.toString());
      String temp;
      while (itr.hasMoreTokens()) {
        // numBerOfWord++;
        temp = itr.nextToken();

        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(temp);
       
        String temp1 = matcher.replaceAll("");
        // System.out.print(temp1+ " ");
        if (!stopWord.containsKey(temp1.toLowerCase())){
          // notStopWord++;
          if (temp1.length() >1 && temp1.charAt(temp1.length() - 1) =='s') {
            String temp2 = temp1.substring(0, temp1.length() - 1);
            int termID = areWordSimilar(temp2);
            int termID1 = areWordSimilar(temp1);
            if (termID != -1 ) {
              word.set(termID + " " + fileId);
              context.write(word, one);
            } else if (termID1 != -1 ) {
              word.set(termID1 + " " + fileId);
              context.write(word, one);
            }
          } else {
            int termID = areWordSimilar(temp1);
            if (termID == -1) {
              // System.out.print(" NT ");
              continue;
            }
            // System.out.print(" YT " + termID + " " );
            word.set(termID + " " + fileId);
            context.write(word, one);
          }
        } else {
          // System.out.print(" SW ");
        }
        
        // if (!stopWord.containsKey(temp)){
        //   // notStopWord++;
        //   int termID = areWordSimilar(temp);
        //   if (termID == -1) {
        //     continue;
        //   }
        //   word.set(termID + " " + fileId);
        //   context.write(word, one);
        // }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  //stop word
  public static  HashMap<String, Boolean> stopWord = new HashMap<>(); 

  public static void addStopWord(String fileStopWords){
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(fileStopWords));
      String line;
      String [] words;
      while ((line = br.readLine()) != null) {
        words = line.split(" ");
        for (String word : words) {
          // System.out.print(word);
          stopWord.put(word, true);
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
  public static int areWordSimilar(String word1) {
        // Chuyển đổi các từ về chữ thường để so sánh
        String temp = word1.toLowerCase();
        // System.out.println(word1+ "aaa");

        for (int j = 0; j < termId.size(); j++) {
          if (temp.equals(termId.get(j))) {
            return j+1;
          }
          // if (word1.contains(termId.get(j)) ) { 
          //   int temp = termId.get(j).length() - word1.length();
          //   if (temp<2 && temp>0) {
          //     return j+1;
          //   }
          // }
        }
        return -1;
    }

  //format output file
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
                        int term = Integer.parseInt(parts[0]);
                        int doc = docId.get(parts[1]);
                        int freq = Integer.parseInt(parts[2]);
                        ouputMTX.add(term + " " + doc + " " + freq + "\n");
                      
                        
                        // int termID = areWordSimilar(parts[0]);
                        // if ( termID != -1) {
                        //   int term = termID;
                        //   int doc = docId.get(parts[1]);
                        //   int freq = Integer.parseInt(parts[2]);
                        //   ouputMTX.add(term + " " + doc + " " + freq + "\n");
                        // } else {
                        //   continue;
                        // }
                    }
                } finally {
                    // Close input stream
                    reader.close();
                    in.close();
                }
            }
            //sort output
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
// public static void combineOutputFiles(String srcDir, String destFile, String confPath) throws IOException {
//     Configuration conf = new Configuration();
//     conf.addResource(new Path(confPath));
//     FileSystem hdfs = FileSystem.get(conf);
//     OutputStream out = hdfs.create(new Path(destFile));
//     Path inputDir = new Path(srcDir);
//     FileStatus[] status = hdfs.listStatus(inputDir);

//     for (int i=0;i<status.length;i++){
//         if (status[i].getPath().getName().startsWith("part-r-")) {
//             InputStream in = hdfs.open(status[i].getPath());
//             try {
//                 IOUtils.copyBytes(in, out, conf, false);
//             } finally {
//                 IOUtils.closeStream(in);
//             }
//         }
//     }
//     IOUtils.closeStream(out);
// }

  public static void main(String[] args) throws Exception {
    addStopWord("../Input/stopwords.txt");
    // if (stopWord.containsKey("adjust")) {
    //   System.out.println("adjust stopword");
    // }

    addDocId("../Input/bbc-preprocess/bbc.docs");
    addTermId("../Input/bbc-preprocess/bbc.terms");

    // System.out.println(areWordSimilar("adjust"));
    // if (areWordSimilar("adjust") != -1) {
    //   System.out.println("adjust term");
    // }

    String outputFileName ="task1_1";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lab2 task1.1");
    job.setJarByClass(task1_1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // job.setOutputFormatClass(MatrixMarketOutputFormat.class);

    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // FileOutputFormat.setOutputName(job, outputFileName);
    if (job.waitForCompletion(true)) {
      
      CombineOutputFiles.outputMTXFiles(args[1]);
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}
