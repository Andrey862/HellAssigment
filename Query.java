import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONObject;
import org.json.JSONException;

public class Query {

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(DoubleWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable key1 = (DoubleWritable) w1;
            DoubleWritable key2 = (DoubleWritable) w2;          
            return -1 * key1.compareTo(key2);
        }
    }

    public static class QueryReducer
            extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
            private Text title = new Text();

            public void reduce(DoubleWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
                for (Text val : values) {
                    title.set(val.toString());
                    context.write(key, title);
                }
                    }
    }

    public static class QueryMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{
            private Text title = new Text();
            private DoubleWritable result = new DoubleWritable();

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                try {
                    JSONObject json = new JSONObject(value.toString());
                    title.set(json.getString("title"));
                    Configuration conf = context.getConfiguration();
                    String query = conf.get("query");
                    query = query.replaceAll("(\\\\n)+", " ");
                    query = query.replaceAll("[^a-zA-Z- ]", "").toLowerCase();
                    StringTokenizer itr = new StringTokenizer(query);
                    Double score = 0.0;
                    while (itr.hasMoreTokens()) {
                        try {
                            score += json.getJSONObject("vector").getDouble(itr.nextToken());
                        } catch (JSONException e) {
                        }
                    }

                    result.set(score);
                    context.write(result, title);
                } catch (JSONException e) {
                }
                    }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: query <index> <out> <n> <query>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("query", args[3]);
        Job job = Job.getInstance(conf, "query");
        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setCombinerClass(QueryReducer.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) {
            try {
                Configuration conf1 = new Configuration();
                FileSystem fs = FileSystem.get(new Configuration());
                Path path = new Path(args[1] + "/part-r-00000");
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = null;
                Integer n = Integer.parseInt(args[2]);
                System.out.println("\nTop Results");
                while ((line = fis.readLine()) != null && n > 0) {
                    System.out.println("\t" + line);
                    --n;
                }
            } catch (IOException ioe) {
                System.err.println("Caught while reading output file '"
                        + StringUtils.stringifyException(ioe));
            }
            System.exit(0);
        }
        System.exit(1);

    }
}
