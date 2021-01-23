package hashtags;

import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserHashtag {
	public static class TPMapper extends Mapper<Object, Text, Text, IntWritable> {

	    @Override
	    public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	    	
	      JsonNode root = new ObjectMapper().readTree(value.toString());
	      
	      if (root.has("delete")) {
	    	  return;
	      }
	      
	      String hashtag = context.getConfiguration().get("hashtag");
	      
	      String user = root.get("user").get("name").asText();
	      ArrayList<String> hashtags = new ArrayList<>(root.get("entities").get("hashtags").findValuesAsText("text"));
	      if (hashtags.contains(hashtag))
	    	  context.write(new Text(user), new IntWritable(1));
	    }
	  }
	
	public static class HashtagCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {        	

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values){
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class HashtagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {        	

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values){
				sum += value.get();
			}
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

    job.setJarByClass(UserHashtag.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[1]));

    // Mapper
    job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setCombinerClass(HashtagCombiner.class);
    // Reducer
    job.setReducerClass(HashtagReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}