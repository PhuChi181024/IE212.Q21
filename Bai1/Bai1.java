import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieId = new Text();
        private Text ratingValue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if(line.isEmpty()) return;
            String[] fields = line.split(",");

            if(fields.length >= 3){
                try {
                    String mId = fields[1].trim();     // MovieID
                    String rating = fields[2].trim();  // Rating
                    movieId.set(mId);
                    ratingValue.set("R|" + rating);
                    context.write(movieId, ratingValue);
                } catch(Exception e) {
                    // bỏ qua dòng lỗi
                }
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text movieId = new Text();
        private Text movieTitle = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if(line.isEmpty()) {
                return;
            }
            String[] parts = line.split(",",3);
            if(parts.length >= 2) {
                String id = parts[0].trim();
                String title = parts[1].trim();
                movieId.set(id);
                movieTitle.set("M|" + title);
                context.write(movieId, movieTitle);
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text>{
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "";
            double sum = 0;
            int count = 0;
            for(Text val : values){
                String data = val.toString();
                if(data.startsWith("R|")){
                    double rating = Double.parseDouble(data.substring(2));
                    sum += rating;
                    count++;
                }
                else if(data.startsWith("M|")){
                    movieTitle = data.substring(2);
                }
            }

            if(count > 0){
                double avg = sum / count;
                outputKey.set(movieTitle);
                outputValue.set("AverageRating: " + String.format("%.2f", avg) + " (TotalRatings: " + count + ")");
                context.write(outputKey, outputValue);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Rating Average");
        job.setJarByClass(Bai1.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}