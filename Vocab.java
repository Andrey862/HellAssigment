import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

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

public class Vocab {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                JSONObject json = new JSONObject(value.toString());
                int id = Integer.parseInt(json.getString("id"));
                String text = json.getString("text").replaceAll("\\\\n", " ").replaceAll("[^a-zA-Z- ]", "").toLowerCase();
                StringTokenizer itr = new StringTokenizer(text);
                while (itr.hasMoreTokens()) {
                    context.write(new Text(itr.nextToken()), new IntWritable(id));
                }
                    }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int count = 0;
                ArrayList<Integer> newList = new ArrayList<Integer>();
                for (IntWritable val : values) {
                    if (!newList.contains(val.get())) {
                        count++;
                        newList.add(val.get());
                    }
                }
                result.set(count);
                context.write(key, result);
                    }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vocab");
        job.setJarByClass(Vocab.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for(int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
