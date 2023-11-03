package org.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DiscogsAnalysis extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DiscogsAnalysis(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "DiscogsAnalysis");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MusicMapper.class);
        job.setCombinerClass(MusicCombiner.class);
        job.setReducerClass(MusicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MusicCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Set<String> genresSet = new HashSet<>();

            for (Text val : values) {
                String[] fields = val.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                count += Integer.parseInt(fields[0]);
                genresSet.add(fields[1]);
            }

            StringBuilder genresBuilder = new StringBuilder();
            for (String genre : genresSet) {
                if (genresBuilder.length() > 0) {
                    genresBuilder.append(",");
                }
                genresBuilder.append(genre);
            }

            result.set(count + "," + genresBuilder.toString());
            context.write(key, result);
        }
    }

    public static class MusicMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        public void map(LongWritable offset, Text lineText, Context context) {
            String line = lineText.toString();
            try {
                if (offset.get() != 0) {
                    String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                    if (fields.length >= 12) {
                        String labelId = fields[6];
                        String artistId = fields[4];
                        String artistName = fields[5];
                        String releaseDate = fields[11];
                        String genre = fields[8];

                        String decade = releaseDate.substring(0, 3) + "0s";

                        String key = labelId + "," + artistId + "," + artistName + "," + decade;
                        String value = "1," + genre;

                        outKey.set(key);
                        outValue.set(value);
                        context.write(outKey, outValue);

                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
                e.printStackTrace();
            }
        }
    }

    public static class MusicReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Set<String> genresSet = new HashSet<>();


            for (Text val : values) {
                String[] fields = val.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                count += Integer.parseInt(fields[0]);

                // Add genres to the set
                genresSet.add(fields[1]);
            }

            // Construct genres string manually to handle commas within genres
            StringBuilder genresBuilder = new StringBuilder();
            for (String genre : genresSet) {
                if (genresBuilder.length() > 0) {
                    genresBuilder.append(",");
                }
                genresBuilder.append(genre);
            }

            result.set(count + "," + genresBuilder.toString());
            context.write(key, result);
        }
    }

}
