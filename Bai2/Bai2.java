import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;
            String movieID = parts[1].trim();
            String rating = parts[2].trim();
            context.write(new Text(movieID), new Text("R," + rating));
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length < 3) return;
            String movieID = parts[0].trim();
            String genres = parts[2].trim();
            context.write(new Text(movieID), new Text("M," + genres));
        }
    }

    // Reducer Job1: join movie và rating
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genres = "";
            List<Double> ratings = new ArrayList<>();
            for (Text val : values) {
                String[] parts = val.toString().split(",", 2);
                if (parts[0].equals("M")) {
                    genres = parts[1];
                }
                else if (parts[0].equals("R")) {
                    ratings.add(Double.parseDouble(parts[1]));
                }
            }
            if (!genres.equals("")) {
                String[] genreList = genres.split("\\|");
                for (String g : genreList) {
                    for (double r : ratings) {
                        context.write(new Text(g), new Text(String.valueOf(r)));
                    }
                }
            }
        }
    }

    // Mapper Job2: đọc kết quả Job1
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    // Reducer Job2: tính average
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;

            }
            double avg = sum / count;
            context.write(key, new Text(String.format("%.2f", avg) + ", TotalRatings: " + count)
            );
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Join Movies and Ratings");
        job1.setJarByClass(Bai2.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        Path tempOutput = new Path("/user/cloudera/temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true))
            System.exit(1);
        
        Job job2 = Job.getInstance(conf, "Genre AverageRating");
        job2.setJarByClass(Bai2.class);
        job2.setMapperClass(GenreMapper.class);
        job2.setReducerClass(GenreReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}