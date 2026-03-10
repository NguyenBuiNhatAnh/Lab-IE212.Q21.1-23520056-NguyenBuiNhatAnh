import java.io.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Text;

public class bai1 {

    // Mapper cho ratings_*.txt
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if(line.isEmpty()) return;

            String[] parts = line.split(",",3);

            if(parts.length < 4) return;

            try {

                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieId);
                ratingValue.set("Rate:" + rating);

                context.write(movieIdKey, ratingValue);

            } catch(Exception e) {
                // ignore lỗi
            }
        }
    }

    // Mapper cho movies.txt
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if(line.isEmpty()) return;

            String[] parts = line.split(",");

            if(parts.length < 3) return;

            String movieId = parts[0].trim();
            String movieName = parts[1].trim();

            movieIdKey.set(movieId);
            movieNameValue.set("Movie:" + movieName);

            context.write(movieIdKey, movieNameValue);
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        private String maxMovie = "";
        private double maxRating = 0;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            String movieName = "";

            for(Text val : values){

                String value = val.toString();

                if(value.startsWith("Rate:")){

                    double rating = Double.parseDouble(value.replace("Rate:",""));
                    sum += rating;
                    count++;

                } else if(value.startsWith("Movie:")){

                    movieName = value.replace("Movie:","");
                }
            }

            if(count == 0) return;

            double avg = sum / count;

            context.write(
                new Text(movieName),
                new Text(String.format("Average Rating: %.2f (Total Ratings: %d)", avg, count))
            );

            if(count >= 5 && avg > maxRating){

                maxRating = avg;
                maxMovie = movieName;
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(
                new Text(maxMovie),
                new Text(String.format(
                    "is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings",
                    maxRating))
            );
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(bai1.class);

        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ratings files
        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                RatingMapper.class
        );

        // movies file
        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                MovieMapper.class
        );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}