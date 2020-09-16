import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;
import java.io.*;

public class Indexer {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text>{

             public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                JSONObject json = new JSONObject(value.toString());
                int id = Integer.parseInt(json.getString("id"));
                String text = json.getString("text").replaceAll("(\\\\n)+", " ").replaceAll("[^a-zA-Z- ]", "").toLowerCase();
                StringTokenizer itr = new StringTokenizer(text);
                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken();
                    if (!word.equals("")&&!word.equals(" ")) 
                        context.write(new IntWritable(id), new Text(word));
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

            public void reduce(IntWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {

                // read idf
                Configuration conf = context.getConfiguration();
                String vocab = conf.get("vocab");
                StringTokenizer vocab_itr = new StringTokenizer(vocab);
                Map<String, Integer> idf = new HashMap<String, Integer>();
                int i=0;
                while (vocab_itr.hasMoreTokens()){
                    String word = vocab_itr.nextToken();
                    int f = Integer.parseInt(vocab_itr.nextToken());
                    idf.put(word, f);
                }
                
                //read doc
                
                Map<String, Integer> tf = new HashMap<String, Integer>();
                   for (Text val : values) {
                    String str = val.toString();
                    tf.put(str, tf.getOrDefault(str, 0)+1);
                }

                // get tf/idf
                Map<String, Double> tfidf = new HashMap<String, Double>();
                for (Map.Entry tf_el : tf.entrySet()) { 
                    String word = (String)tf_el.getKey();
                    tfidf.put(word, tf.get(word)*1.0/idf.getOrDefault(word,1));
                }

                String result = "";
                
                for (Map.Entry tfidf_el : tfidf.entrySet()) {
                    if (!tfidf_el.getKey().equals("")&&!tfidf_el.getKey().equals(" ")){
                        result+=tfidf_el.getKey()+" "+tfidf_el.getValue()+" ";
                    }
                }
                
                context.write(key, new Text(result));
            }
    }

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream is = fs.open(new Path(args[0]));

        Scanner s = new Scanner(is).useDelimiter("\\A");
        String vaocab = s.hasNext() ? s.next() : "";

        Configuration conf = new Configuration();
        conf.set("vocab", vaocab);
        Job job = Job.getInstance(conf, "indexer");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        for(int i = 1; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
