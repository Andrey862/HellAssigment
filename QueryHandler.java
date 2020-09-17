import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class QueryHandler {

    public static class TokenizerMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{

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
                    context.write(new DoubleWritable(result),new Text(Integer.toString(docId)));
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

            public void reduce(DoubleWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
                //read titles
                Configuration conf = context.getConfiguration();
                String titles_text = conf.get("titles");
                Map<Integer, String> titles = new HashMap<Integer, String>();
                
                String[] titles_lines = titles_text.split("\n");
                for (String line : titles_lines){
                    StringTokenizer titles_itr = new StringTokenizer(line);
                    int id = Integer.parseInt(titles_itr.nextToken());
                    String title = "";
                    while(titles_itr.hasMoreTokens())
                        title+=titles_itr.nextToken();
                    titles.put(id, title);
                }
                
                // change id to titels
                for (Text val: values){
                        int id = Integer.parseInt(val.toString().replaceAll("[^0-9]", ""));
                        context.write(key, new Text(titles.get(id)));
                    }
                }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query", args[3]);

        FileSystem fs = FileSystem.get(new Configuration());
        InputStream is = fs.open(new Path(args[0]));

        Scanner s = new Scanner(is).useDelimiter("\\A");
        String titles = s.hasNext() ? s.next() : "";
        conf.set("titles", titles);

        Job job = Job.getInstance(conf, "queryHandler");
        job.setJarByClass(QueryHandler.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
