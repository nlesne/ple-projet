package users;

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

public class AllHastagUser {

	public static class UserCombiner extends Reducer<LongWritable, Tweet, LongWritable, Tweet> {        
		private int k = 10;
		
		@Override
		protected void setup(Reducer<LongWritable, Tweet, LongWritable, Tweet>.Context context)
				throws IOException, InterruptedException {
          this.k = context.getConfiguration().getInt("k", 10);
		} 

    public void reduce(LongWritable key, Iterable<Tweet> values, Context context)
     throws IOException, InterruptedException {
			for(Tweet tweet : values){
        if((int)tweet.getUserId() == k)
          context.write(new LongWritable(tweet.getUserId()), tweet);
      }
		}
	}

	public static class UserReducer extends Reducer<LongWritable,Tweet,LongWritable,Text> {

    public void reduce(LongWritable key, Iterable<Tweet> values,Context context) 
    throws IOException, InterruptedException {
			ArrayList<String> allHastag = new ArrayList<String>();
			for (Tweet tweet : values) {
        ArrayList<String> list = tweet.getHastags();
        for(String hash : list){
          if(!allHastag.contains(hash))
            allHastag.add(hash);
        }
      }
      for(String hash : allHastag){
        context.write(key, new Text(hash));
      }
		}
	}
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
		int k = 0;
		k = Integer.parseInt(args[0]);
		conf.setInt("k", k);


    Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(AllHastagUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		//Mapper
    job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Tweet.class);
        
		//Combiner
		job.setCombinerClass(UserCombiner.class);

		//Reducer
		job.setReducerClass(UserReducer.class);
    job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

public static class TPMapper extends Mapper<Object, Text, LongWritable, Tweet> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

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