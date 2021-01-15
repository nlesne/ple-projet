package hastags;

import parser.*;
import parser.TweetParser.TPMapper;

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
	public static class HastagReducer extends Reducer<LongWritable, Tweet, Text, LongWritable> {        
		private static String hastag = " ";
		
		@Override
		protected void setup(Reducer<LongWritable, Tweet, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			this.hastag = context.getConfiguration().get("hastag", " ");
		} 

		public void reduce(LongWritable key, Iterable<Tweet> values, Context context) throws IOException, InterruptedException {
			for(Tweet tweet : values){
				ArrayList<String> listHashtag = tweet.getHastags();
				int size = listHashtag.size();
				for(int i=0; i<size; i++){
					String word = listHashtag.get(i);
					if(word == hastag)
						context.write(new Text(word), new LongWritable(tweet.getUserId()));
				}
			}
		}
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String hastag = "";
    hastag = args[0];
    conf.set("hastag", hastag);

    Job job = Job.getInstance(conf, "TwitterProject");
    job.setNumReduceTasks(1);

    job.setJarByClass(UserHastag.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[1]));

    // Mapper
    job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Reducer
    job.setReducerClass(HastagReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}