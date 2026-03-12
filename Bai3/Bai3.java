import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Bai3 {

    // JOB 1: Join ratings + users 
    public static class RatingMapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();   // userId
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split(",\\s*");
            if (parts.length != 4) {
                return;
            }
            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();
            outKey.set(userId);
            outValue.set("R\t" + movieId + "\t" + rating);
            context.write(outKey, outValue);
        }
    }

    public static class UserMapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();   // userId
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split(",\\s*");
            if (parts.length < 2) {
                return;
            }
            String userId = parts[0].trim();
            String gender = parts[1].trim();
            outKey.set(userId);
            outValue.set("U\t" + gender);
            context.write(outKey, outValue);
        }
    }

    public static class JoinReducerJob1 extends Reducer<Text, Text, Text, Text> {
        private String currentGender = null;
        @Override
        protected void reduce(Text userId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            currentGender = null;
            // Tìm gender của user
            for (Text val : values) {
                String line = val.toString();
                if (line.startsWith("U\t")) {
                    currentGender = line.substring(2).trim();
                    break; 
                }
            }
            if (currentGender == null) {
                return; // 
            }
            // Pass 2: Lấy tất cả rating của user
            for (Text val : values) {
                String line = val.toString();
                if (line.startsWith("R\t")) {
                    String[] parts = line.split("\t", 3);
                    if (parts.length != 3) continue;
                    String movieId = parts[1];
                    String rating = parts[2];
                    context.write(new Text(movieId), new Text(currentGender + "\t" + rating));
                }
            }
        }
    }

    // JOB 2: Join với movies và tính trung bình rating theo giới tính

    public static class JoinDataMapperJob2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\t", 2);
            if (parts.length != 2) {
                return;
            }
            String movieId = parts[0].trim();
            String data = parts[1].trim();
            context.write(new Text(movieId), new Text("D\t" + data));
        }
    }

    public static class MovieMapperJob2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split(",\\s*", 3);
            if (parts.length < 2) {
                return;
            }
            String movieId = parts[0].trim();
            String title = parts[1].trim();
            context.write(new Text(movieId), new Text("M\t" + title));
        }
    }

    public static class AvgReducerJob2 extends Reducer<Text, Text, Text, Text> {
        private double maleSum = 0;
        private int maleCount = 0;
        private double femaleSum = 0;
        private int femaleCount = 0;
        private String movieTitle = null;

        @Override
        protected void reduce(Text movieId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            maleSum = 0;
            maleCount = 0;
            femaleSum = 0;
            femaleCount = 0;
            movieTitle = null;
            for (Text val : values) {
                String line = val.toString().trim();
                if (line.startsWith("M\t")) {
                    movieTitle = line.substring(2).trim();
                } else if (line.startsWith("D\t")) {
                    String data = line.substring(2).trim();
                    String[] parts = data.split("\t", 2);
                    if (parts.length != 2) continue;
                    String gender = parts[0].trim();
                    double rating;
                    try {
                        rating = Double.parseDouble(parts[1].trim());
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    if ("M".equals(gender)) {
                        maleSum += rating;
                        maleCount++;
                    } else if ("F".equals(gender)) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }
            if (movieTitle == null || (maleCount == 0 && femaleCount == 0)) {
                return;
            }
            double maleAvg   = 0.0;
            if (maleCount > 0) {
                maleAvg = maleSum / maleCount;
            }
            double femaleAvg = 0.0;
            if (femaleCount > 0) {
                femaleAvg = femaleSum / femaleCount;
            }
            String result = String.format("%s Male: %.2f, Female: %.2f",
                    movieTitle, maleAvg, femaleAvg);

            context.write(new Text(movieTitle), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Join Ratings + Users");
        job1.setJarByClass(Bai3.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapperJob1.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapperJob1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(JoinReducerJob1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        Path tempPath = new Path(args[3] + "_temp");
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Gender Average Rating");
        job2.setJarByClass(Bai3.class);
        MultipleInputs.addInputPath(job2, tempPath, TextInputFormat.class, JoinDataMapperJob2.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapperJob2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(AvgReducerJob2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}