import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class Titler {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text>{

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                JSONObject json = new JSONObject(value.toString());
                int id = Integer.parseInt(json.getString("id"));
                String title = json.getString("title");
                context.write(new IntWritable(id), new Text(title));
                }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

            public void reduce(IntWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
                
                for (Text val : values) 
                    context.write(key, val);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "titler");
        
        job.setJarByClass(Titler.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        for(int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
