import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bigram_code {

    // Mapper Class
    public static class InvertedIndexBigramMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text documentId = new Text();
        private final Text bigramKey = new Text();

        private static final String[] TARGET_BIGRAMS = {
                "computer science", "information retrieval",
                "power politics", "los angeles", "bruce willis"
        };

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);

            if (parts.length < 2) return;

            documentId.set(parts[0]);

            String[] words = cleanAndSplitText(parts[1]);

            emitTargetBigrams(words, context);
        }

        private String[] cleanAndSplitText(String text) {
            return text.toLowerCase()
                       .replaceAll("[^a-z]+", " ")
                       .trim()
                       .split("\\s+");
        }

        private void emitTargetBigrams(String[] words, Context context) throws IOException, InterruptedException {
            for (int i = 0; i < words.length - 1; i++) {
                String bigram = words[i] + " " + words[i + 1];
                if (isTargetBigram(bigram)) {
                    bigramKey.set(bigram);
                    context.write(bigramKey, documentId);
                }
            }
        }

        private boolean isTargetBigram(String bigram) {
            for (String target : TARGET_BIGRAMS) {
                if (bigram.equals(target)) {
                    return true;
                }
            }
            return false;
        }
    }

    // Reducer Class
    public static class InvertedIndexBigramReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outputValue = new Text();

        @Override
        public void reduce(Text bigram, Iterable<Text> documentIds, Context context)
                throws IOException, InterruptedException {

            HashMap<String, Integer> documentFrequencyMap = countDocumentFrequencies(documentIds);

            String frequencyString = buildFrequencyString(documentFrequencyMap);

            outputValue.set(frequencyString);
            context.write(bigram, outputValue);
        }

        private HashMap<String, Integer> countDocumentFrequencies(Iterable<Text> documentIds) {
            HashMap<String, Integer> frequencyMap = new HashMap<>();

            for (Text documentId : documentIds) {
                String doc = documentId.toString();
                frequencyMap.put(doc, frequencyMap.getOrDefault(doc, 0) + 1);
            }

            return frequencyMap;
        }

        private String buildFrequencyString(HashMap<String, Integer> frequencyMap) {
            StringBuilder frequencyBuilder = new StringBuilder();

            for (String doc : frequencyMap.keySet()) {
                frequencyBuilder.append(doc)
                                .append(":")
                                .append(frequencyMap.get(doc))
                                .append(" ");
            }

            return frequencyBuilder.toString().trim();
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: bigram_code <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index Bigram");

        job.setJarByClass(bigram_code.class);
        job.setMapperClass(InvertedIndexBigramMapper.class);
        job.setReducerClass(InvertedIndexBigramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
