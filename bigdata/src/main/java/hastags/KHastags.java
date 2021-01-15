package hastags;

import parser.*;
import parser.TweetParser.TPMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KHastags {

	public static class Hastags implements Writable {
		
		public String name;
		public int total;
		
		public Hastags() {
			this.name = "";
			this.total = 0;
		}
		
		public Hastags(String name, int total) {
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

	public static class CountOneHastagCombiner extends Reducer<LongWritable, Tweet, Text, IntWritable> {
		public void reduce(LongWritable key, Iterable<Tweet> values, Context context) throws IOException, InterruptedException {
			for(Tweet tweet : values){
				ArrayList<String> listHashtag = tweet.getHastags();
				int size = listHashtag.size();
				for(int i=0; i<size; i++){
					String word = listHashtag.get(i);
					context.write(new Text(word), new IntWritable(1));
				}
			}
		}
	}

	public static class CountAllHastagCombiner extends Reducer<Text,IntWritable,NullWritable,Hastags> {

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			Hastags hastags = new Hastags(key.toString(), sum);
			context.write(NullWritable.get(), hastags);
		}
	}

	public static class TopKHastagCombiner extends Reducer<NullWritable,Hastags,NullWritable,Hastags> {

		private int k = 10;
		
		@Override
		protected void setup(Reducer<NullWritable, Hastags, NullWritable, Hastags>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		public void reduce(Text key, Iterable<Hastags> values,Context context) throws IOException, InterruptedException {
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(Hastags hash : values) {
				topk.put(hash.total, hash.name);
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(NullWritable.get(), new Hastags(v.getValue(), v.getKey()));
			}
		}
	} 

	public static class TopKHastagReducer extends Reducer<NullWritable,Hastags,NullWritable,Hastags> {

		private int k = 10;
		
		@Override
		protected void setup(Reducer<NullWritable, Hastags, NullWritable, Hastags>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		public void reduce(NullWritable key, Iterable<Hastags> values,Context context) throws IOException, InterruptedException {
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(Hastags hash : values) {
				topk.put(hash.total, hash.name);
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(NullWritable.get(), new Hastags(v.getValue(), v.getKey()));
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

		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));
		
		//Mapper
		job.setMapperClass(TPMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Tweet.class);
		
		job.setJarByClass(KHastags.class);
		//Combiner
		job.setCombinerClass(CountOneHastagCombiner.class);
		job.setCombinerClass(CountAllHastagCombiner.class);
		job.setCombinerClass(TopKHastagCombiner.class);
		//Reducer
		job.setReducerClass(TopKHastagReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);			
		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  }
		  catch (Exception e) {
			System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
		  }
		  int res = -1;
		  res = job.waitForCompletion(true) ? 0 : 1;
		  System.exit(res);
  }
}