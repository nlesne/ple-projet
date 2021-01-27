package users;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hbase.Utils;

public class TweetCountry {
	private final static String countryTableName = Utils.tablePrefix + "tweetsByCountry";

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

	public static class UserReducer extends TableReducer<Text, IntWritable, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values){
				sum += val.get();
			}
            String rowPrefix = context.getConfiguration().get("rowDate");
            String rowName = rowPrefix + "-" + key.toString();
			Put put = new Put(rowName.getBytes());
			put.addColumn(Bytes.toBytes(Utils.famName), Bytes.toBytes(Utils.colName), Bytes.toBytes(sum));
			context.write(new Text(rowName), put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);
		job.setJarByClass(TweetCountry.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		Path inputPath = new Path(args[0]);
		String[] dateParts = inputPath.getName().split("_");
		String rowDate = dateParts[1] + "_" + dateParts[2];
		conf.set("rowDate", rowDate);
		TextInputFormat.addInputPath(job, inputPath);
		
		Configuration hbaseConf  = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hbaseConf);
		Utils.createTable(connection, countryTableName);

		//Mapper
		job.setMapperClass(TPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//Combiner
		job.setCombinerClass(UserCombiner.class);

		//Reducer
		job.setReducerClass(UserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Put.class);

		TableMapReduceUtil.initTableReducerJob(countryTableName, UserReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}