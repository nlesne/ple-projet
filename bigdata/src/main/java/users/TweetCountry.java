package users;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TweetCountry {

  public static class TPMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
      JsonNode root = new ObjectMapper().readTree(value.toString());
      
      if (root.has("delete")) {
    	  return;
      }

      String country = new String("");
      if (root.hasNonNull("place")) {
    	  country = root.get("place").get("country").asText();
        context.write(new Text(country), new IntWritable(1));
      }
    }
  }

	public static class UserCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {        

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
     throws IOException, InterruptedException {
      int sum = 0;
		  for (IntWritable val : values){
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
		}
	}

	public static class UserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,Context context) 
    throws IOException, InterruptedException {
      int sum = 0;
		  for (IntWritable val : values){
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
	}
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(TweetCountry.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));

		//Mapper
    job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
        
		//Combiner
		job.setCombinerClass(UserCombiner.class);

		//Reducer
		job.setReducerClass(UserReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}