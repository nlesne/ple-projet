package hashtags;

import parser.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KHashtags {

	public static class Hashtags implements Writable {

		public String name;
		public int total;

		public Hashtags() {
			this.name = "";
			this.total = 0;
		}

		public Hashtags(String name, int total) {
			this.name = name;
			this.total = total;
		}

		public void readFields(DataInput in) throws IOException {
			this.name = in.readUTF();
			this.total = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(name);
			out.writeInt(total);
		}
	}

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			JsonNode root = new ObjectMapper().readTree(value.toString());

			if (root.has("delete")) {
				return;
			}

			ArrayList<String> hashtags = new ArrayList<>(root.get("entities").get("hashtags").findValuesAsText("text"));
			for (String hash : hashtags) {
				context.write(new Text(hash), new IntWritable(1));
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Hashtags> {

		private TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
    private int k = 10;
    
    @Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Hashtags>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\s+");
			int tot = Integer.valueOf(tokens[1]);
			String name = tokens[0];
			topk.put(tot, name);
			
			while (topk.size() > k)
				topk.remove(topk.firstKey());

			for(Map.Entry<Integer,String> pair : topk.entrySet()) {
				Hashtags h = new Hashtags(pair.getValue(), pair.getKey());
				context.write(NullWritable.get(), h);
			}
		}
	}

	public static class TopKReducer extends Reducer<NullWritable, Hashtags, IntWritable, Text> {
    private int k = 10;
    
		protected void setup(Mapper<NullWritable, Hashtags, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<Hashtags> values, Reducer<NullWritable, Hashtags, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(Hashtags cp : values) {
				//insertion de la ville
				topk.put(cp.total, cp.name);
				// on conserve les k plus grandes
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			
			// ecrire les k plus grandes
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(new IntWritable(v.getKey()), new Text(v.getValue()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

		//////////////Job 1
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		job.setJarByClass(KHashtags.class);
		// Mapper
		job.setMapperClass(CountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Reducer
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path outputPath = new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);


		//////////Job 2
    Configuration conf2 = new Configuration();
    
    int k = 0;
		k = Integer.parseInt(args[0]);
    conf2.setInt("k", k);
    
		Job job2 = Job.getInstance(conf2);
		job2.setNumReduceTasks(1);

		TextInputFormat.addInputPath(job2, outputPath) ;

		job2.setJarByClass(KHashtags.class);

		job2.setMapperClass(TopKMapper.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Hashtags.class);

		// Reducer
		job2.setReducerClass(TopKReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

