import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class QueryHandler {

    public static class TokenizerMapper
            extends Mapper<Object, Text, DoubleWritable, IntWritable>{

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                
                //read a line in indexer file
                String[] lines = value.toString().split(System.getProperty("line.separator"));
                
                for (String value_line :lines){
                    //read indexer line
                    StringTokenizer tfidf = new StringTokenizer(value_line);
                    int docId = Integer.parseInt(tfidf.nextToken());
                    Map<String, Double> weights = new HashMap<String, Double>();
                    while (tfidf.hasMoreTokens()){
                        String word = tfidf.nextToken();
                        if (!tfidf.hasMoreTokens()) break;
                        double weight = Double.parseDouble(tfidf.nextToken());
                        weights.put(word, weight);
                    }
                    //read query
                    Configuration conf = context.getConfiguration();
                    String query = conf.get("query");
                    StringTokenizer query_itr = new StringTokenizer(query);
                    // compute
                    double result = 0;
                    while (query_itr.hasMoreTokens())
                        result+=weights.getOrDefault(query_itr.nextToken(), 0.0); 
                    context.write(new DoubleWritable(result),new IntWritable(docId));
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(DoubleWritable key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                for (IntWritable val: values)
                    context.write(key, val);
                }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query", args[2]);

        Job job = Job.getInstance(conf, "queryHandler");
        job.setJarByClass(QueryHandler.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
