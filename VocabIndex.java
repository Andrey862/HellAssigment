import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
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

public class VocabIndex {

    public static class VocabMapper
            extends Mapper<Object, Text, Text, IntWritable>{
            // Making Vocabulary
            
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                try {
                    //read the document
                    JSONObject json = new JSONObject(value.toString());
                    int id = Integer.parseInt(json.getString("id"));
                    String text = json.getString("text");
                    //remove non-letters, make everything to lower case
                    text = text.replaceAll("(\\\\n)+", " ");
                    text = text.replaceAll("[^a-zA-Z- ]", "").toLowerCase();
                    StringTokenizer itr = new StringTokenizer(text);
                    while (itr.hasMoreTokens()) {
                        // word -> docid
                        context.write(new Text(itr.nextToken()), new IntWritable(id));
                    }
                } catch (JSONException e) {
                }
                    }
    }

    public static class VocabReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                
                // count in how many documents a word appeared
                HashSet<Integer> set = new HashSet<Integer>();
                for (IntWritable val : values) {
                    if (!set.contains(val.get())) {
                        set.add(val.get());
                    }
                }
                result.set(set.size());
                context.write(key, result);
            }
    }

    public static class IndexMapper
            extends Mapper<Object, Text, Text, Text>{
            private Text word = new Text();
            private Text doc = new Text();
            // Making Indexer

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                try {
                    //same as Vocab mapper but key and value are swapped
                    // read documents
                    JSONObject json = new JSONObject(value.toString());
                    JSONObject docJSON = new JSONObject(json, new String[]{"id", "title"});
                    doc.set(docJSON.toString());
                    String text = json.getString("text");
                    //remove non-letters, change to lowercase
                    text = text.replaceAll("(\\\\n)+", " ");
                    text = text.replaceAll("[^a-zA-Z- ]", "").toLowerCase();
                    StringTokenizer itr = new StringTokenizer(text);
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        // docid -> word
                        context.write(doc, word);
                    }
                } catch (JSONException e) {
                }
            }
    }

    public static class IndexReducer
            extends Reducer<Text,Text,Text,NullWritable> {
            private Text result = new Text();

            private Map<String, Integer> idfs = new HashMap<String, Integer>();
            private Configuration conf;
            private BufferedReader fis;

            @Override
            public void setup(Context context) throws IOException,
                   InterruptedException {
                       //read the Vocab file as cache
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
                    // read dictionary of (word-># of documents where the word occured)
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
                // calculates idf/tf

                Map<String, Integer> tfs = new HashMap<String, Integer>();
                Map<String, Double> vector = new HashMap<String, Double>();

                // calculates tf
                for (Text val : values) {
                    String word = val.toString();
                    tfs.put(word, tfs.getOrDefault(word, 0)+1);
                }

                // calculates tf/idf^2 for each word in document
                for (Map.Entry<String, Integer> entry : tfs.entrySet()) { 
                    String word = entry.getKey();
                    Double idf = idfs.getOrDefault(word, 1) * 1.0;
                    Double tfidf =  tfs.get(word)*1.0/(idf * idf);
                    vector.put(word, tfidf);
                }

                try {
                    //write the result
                    JSONObject docJSON = new JSONObject(key.toString());
                    docJSON.put("vector", new JSONObject(vector));
		            result.set(docJSON.toString());
                    context.write(result, NullWritable.get());
                } catch (JSONException e) {
                }

            }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: index <in> <vocabout> <indexout>");
            System.exit(2);
        }
        // run Vocab
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "vocab");
        job1.setJarByClass(VocabIndex.class);
        job1.setMapperClass(VocabMapper.class);
        job1.setReducerClass(VocabReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        for (int i=0; i < args.length - 2; ++i) {
            FileInputFormat.addInputPath(job1, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job1, new Path(args[args.length-2]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // run Indexer
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "index");
        job2.setJarByClass(VocabIndex.class);
        job2.setMapperClass(IndexMapper.class);
        job2.setReducerClass(IndexReducer.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.addCacheFile(new Path(args[args.length-2] + "/part-r-00000").toUri());
        for (int i=0; i < args.length - 2; ++i) {
            FileInputFormat.addInputPath(job2, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job2, new Path(args[args.length-1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
