package users;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hbase.Utils;

public class TweetLanguage {
	private final static String tableName = Utils.tablePrefix + "tweetsByLang";
	
	public static class TPMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JsonNode root = new ObjectMapper().readTree(value.toString());

			if (root.has("delete")) {
				return;
			}
			String lang = root.get("lang").asText();
			context.write(new Text(lang), new IntWritable(1));
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

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values){
				sum += val.get();
			}
			String rowPrefix = context.getConfiguration().get("rowDate");
			String rowName = rowPrefix + "-" + key.toString();
			Put put = new Put(rowName.getBytes());
			put.addColumn(Bytes.toBytes(Utils.famName), Bytes.toBytes(Utils.colName), Bytes.toBytes(Integer.toString(sum)));
			context.write(new Text(rowName), put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Utils.configRowPrefix(conf, args[0]);
		Job job = Job.getInstance(conf, "TwitterProject");
		job.setNumReduceTasks(1);

		job.setJarByClass(TweetLanguage.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		
		Configuration hbaseConf  = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hbaseConf);
		Utils.createTable(connection, tableName);

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

		TableMapReduceUtil.initTableReducerJob(tableName, UserReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}