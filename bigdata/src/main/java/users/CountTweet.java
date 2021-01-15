
package users;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import parser.Tweet;
import parser.TweetParser.TPMapper;

public class CountTweet {

	public static class UserCombiner extends Reducer<LongWritable, Tweet, LongWritable, IntWritable> {        
		private long user;
		
		@Override
		protected void setup(Reducer<LongWritable, Tweet, LongWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
          this.user = context.getConfiguration().getLong("user", 0);
		} 

    public void reduce(LongWritable key, Iterable<Tweet> values, Context context)
     throws IOException, InterruptedException {
    	for(Tweet tweet : values){
    		if(tweet.getUserId() == user) {
    			System.out.println(tweet);
    			context.write(new LongWritable(tweet.getUserId()), new IntWritable(1));
    		}
    	}
    }
	}

	public static class UserReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {

    public void reduce(LongWritable key, Iterable<IntWritable> values,Context context) 
    throws IOException, InterruptedException {
      int sum = 0;
		  for (IntWritable val : values)
        sum += val.get();
      context.write(key, new IntWritable(sum));
    }
	}
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
		long user = 0;
		user = Long.parseUnsignedLong(args[0]);
		conf.setLong("user", user);


    Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(CountTweet.class);

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
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + user));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}