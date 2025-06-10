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

public class unigram_code {

    // Mapper Class
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text docId = new Text();
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();
            String[] tokens = splitLine(inputLine);

            if (tokens.length < 2) return; // Skip invalid lines

            docId.set(tokens[0]);
            tokenizeAndEmit(tokens[1], context);
        }

        private String[] splitLine(String line) {
            return line.split("\t", 2);
        }

        private void tokenizeAndEmit(String text, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(cleanText(text));
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, docId);
            }
        }

        private String cleanText(String text) {
            return text.toLowerCase().replaceAll("[^a-z]+", " ");
        }
    }

    // Reducer Class
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outputValue = new Text();

        @Override
        public void reduce(Text word, Iterable<Text> docIds, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> docFrequencyMap = buildFrequencyMap(docIds);
            String resultString = formatFrequencyMap(docFrequencyMap);
            outputValue.set(resultString);
            context.write(word, outputValue);
        }

        private HashMap<String, Integer> buildFrequencyMap(Iterable<Text> docIds) {
            HashMap<String, Integer> freqMap = new HashMap<>();
            for (Text docId : docIds) {
                String docName = docId.toString();
                freqMap.put(docName, freqMap.getOrDefault(docName, 0) + 1);
            }
            return freqMap;
        }

        private String formatFrequencyMap(HashMap<String, Integer> freqMap) {
            StringBuilder sb = new StringBuilder();
            for (String doc : freqMap.keySet()) {
                sb.append(doc).append(":").append(freqMap.get(doc)).append(" ");
            }
            return sb.toString().trim();
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: unigram_code <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(unigram_code.class);
        job.setJobName("Inverted Index");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
