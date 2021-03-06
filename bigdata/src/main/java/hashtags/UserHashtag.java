package hashtags;

import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hbase.Utils;

public class UserHashtag {
	private final static String tableName = Utils.tablePrefix + "hashtagUsers";

	public static class TPMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JsonNode root = new ObjectMapper().readTree(value.toString());

			if (root.has("delete")) {
				return;
			}

			String user = root.get("user").get("screen_name").asText();
			ArrayList<String> hashtags = new ArrayList<>(root.get("entities").get("hashtags").findValuesAsText("text"));
			for(String h : hashtags)
				context.write(new Text(h), new Text(user));
		}
	}

	public static class HashtagCombiner extends Reducer<Text, Text, Text, Text> {        	

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			for (Text value : values) {
				String val = value.toString();
				if (!list.contains(val))
					list.add(val);
			}
			for(String user : list)
				context.write(key, new Text(user));
		}
	}

	public static class HashtagReducer extends TableReducer<Text, Text, Text> {        	

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			for (Text value : values) {
				String val = value.toString();
				if (!list.contains(val))
					list.add(val);
			}
			String users = String.join(" ", list);

			String rowPrefix = context.getConfiguration().get("rowDate");
			String rowName = rowPrefix + "-" + key.toString();
			Put put = new Put(rowName.getBytes());
			put.addColumn(Bytes.toBytes(Utils.famName), Bytes.toBytes(Utils.colName), Bytes.toBytes(users));
			context.write(new Text(rowName), put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Utils.configRowPrefix(conf, args[0]);

		Configuration hbaseConf  = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hbaseConf);
		Utils.createTable(connection, tableName);

		Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(UserHashtag.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Mapper
		job.setMapperClass(TPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(HashtagCombiner.class);
		// Reducer
		job.setReducerClass(HashtagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Put.class);

		TableMapReduceUtil.initTableReducerJob(tableName, HashtagReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}