package parser;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TweetParser extends Configured implements Tool {

  public static class TPMapper extends Mapper<Object, Text, LongWritable, Tweet> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
      JsonNode root = new ObjectMapper().readTree(value.toString());
      
      if (root.has("delete")) {
    	  return;
      }

      long tweet_id = root.get("id").asLong();
      String created_at = root.get("created_at").asText();
      String text = root.get("text").asText();
      text = text.replace("\n", "");
      long user_id = root.get("user").get("id").asLong();
      int retweet_count = root.get("retweet_count").asInt();
      String country = new String("");
      if (root.hasNonNull("place")) {
    	  country = root.get("place").get("country").asText();
      }
      String lang = root.get("lang").asText();
      ArrayList<String> hashtags = new ArrayList<>(root.get("entities").get("hashtags").findValuesAsText("text"));
      
      Tweet tweet = new Tweet(created_at, text, user_id, retweet_count, country, lang, hashtags);
      context.write(new LongWritable(tweet_id), tweet);
    }

  }
  

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "Parse Tweet");
    job.setJarByClass(TweetParser.class);
    job.setNumReduceTasks(0);
    job.setMapperClass(TPMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Tweet.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    try {
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
    }
    catch (Exception e) {
      System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
      return -1;
    }

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TweetParser(), args));
  }
}
