package hastags;

import parser.*;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserHastag {
  public static class CountOneHastagCombiner extends Reducer<LongWritable, Tweet, LongWritable, IntWritable> {

    public void reduce(LongWritable key, Iterable<Tweet> values, Context context)
        throws IOException, InterruptedException {
      for (Tweet tweet : values) {
        ArrayList<String> listHashtag = tweet.getHastags();
        int size = listHashtag.size();
        Long user = tweet.getUserId();
        context.write(new LongWritable(user), new IntWritable(size));
      }
    }
  }

  public static class CountHastagReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum != 0)
        context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String hastag = "";
    hastag = args[0];
    conf.set("hastag", hastag);

    Job job = Job.getInstance(conf, "TwitterProject");
    job.setNumReduceTasks(1);

    job.setJarByClass(CountHastags.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[1]));

    // Mapper
    job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Combiner
    job.setCombinerClass(CountOneHastagCombiner.class);

    // Reducer
    job.setReducerClass(CountHastagReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TPMapper extends Mapper<Object, Text, LongWritable, Tweet> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      long tweet_id = 1;
      String created_at = "coucou";
      String text = "coucou";
      long user_id = 1;
      int retweet_count = 1;

      ArrayList<String> hashtags = new ArrayList<String>();

      Tweet tweet = new Tweet(created_at, text, user_id, retweet_count, hashtags);

      context.write(new LongWritable(tweet_id), tweet);
    }

  }

}