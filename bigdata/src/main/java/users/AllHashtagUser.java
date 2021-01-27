package users;

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

public class AllHashtagUser {
	public static class TPMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			JsonNode root = new ObjectMapper().readTree(value.toString());

			if (root.has("delete")) {
				return;
			}

			String nameUser = context.getConfiguration().get("nameUser");

			String user = root.get("user").get("name").asText();
			ArrayList<String> hashtags = new ArrayList<>(root.get("entities").get("hashtags").findValuesAsText("text"));
			if (user.equals(nameUser)){
				for(String h : hashtags)
					context.write(new Text(user), new Text(h));
			}
		}
	}

	public static class HashtagCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			for (Text value : values) {
				String val = value.toString();
				if (!list.contains(val))
					list.add(val);
			}
			for(String h : list)
				context.write(key, new Text(h));
		}
	}

	public static class HashtagReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			for (Text value : values) {
				String val = value.toString();
				if (!list.contains(val))
					list.add(val);
			}
			for(String h : list)
				context.write(key, new Text(h));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String nameUser = "";
		nameUser = args[0];
		conf.set("nameUser", nameUser);

		Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(AllHashtagUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		// Mapper
		job.setMapperClass(TPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(HashtagCombiner.class);
		// Reducer
		job.setReducerClass(HashtagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + nameUser));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}