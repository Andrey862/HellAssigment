import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONObject;

public class Indexer1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
            private Text word = new Text();
            private Text docID = new Text();

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                JSONObject json = new JSONObject(value.toString());
                docID.set(json.getString("id"));
                StringTokenizer itr = new StringTokenizer(json.getString("text").replaceAll("[^a-zA-Z- ]", "").toLowerCase());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(docID, word);
                }
                    }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
            private Text result = new Text();

            private Map<String, Integer> idfs = new HashMap<String, Integer>();
            private Configuration conf;
            private BufferedReader fis;

            @Override
            public void setup(Context context) throws IOException,
                   InterruptedException {
                       conf = context.getConfiguration();
                       URI[] vocabURIs = Job.getInstance(conf).getCacheFiles();
                       for (URI vocabURI : vocabURIs) {
                           Path vocabPath = new Path(vocabURI.getPath());
                           String vocabFileName = vocabPath.getName().toString();
                           vocabFile(vocabFileName);
                       }
            }

            private void vocabFile(String fileName) {
                try {
                    fis = new BufferedReader(new FileReader(fileName));
                    String line = null;
                    while ((line = fis.readLine()) != null) {
                        String[] items = line.split("\\s+");
                        idfs.put(items[0], Integer.parseInt(items[1]));
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '"
                            + StringUtils.stringifyException(ioe));
                }
            }

            @Override
            public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
                Map<String, Integer> tfs = new HashMap<String, Integer>();

                StringBuilder stringBuilder = new StringBuilder();
                for (Text val : values) {
                    String word = val.toString();
                    if (idfs.get(word) != null) {
                        tfs.put(word, tfs.getOrDefault(word, 0)+1);
                    }
                }

                for (Map.Entry<String, Integer> entry : tfs.entrySet()) { 
                    String word = entry.getKey();
                    Double idf = idfs.get(word) * 1.0;
                    Double tfidf =  tfs.get(word)*1.0/(idf * idf);
                    stringBuilder.append(word);
                    stringBuilder.append(" ");
                    stringBuilder.append(tfidf);
                    stringBuilder.append(" ");
                }
                result.set(stringBuilder.toString());
                context.write(key, result);

                    }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        if (args.length < 3) {
            System.err.println("Usage: wordcount <vocab> <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "index");
        job.setJarByClass(Indexer1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(args[0]).toUri());
        for (int i=1; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
