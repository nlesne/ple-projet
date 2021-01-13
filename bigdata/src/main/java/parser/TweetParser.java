package parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

public class TweetParser extends Configured implements Tool {

  public static class TPMapper extends Mapper<Object, Text, LongWritable, Tweet> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      JsonParser parser = new JsonParser();

      Object obj = parser.parse(value.toString());
      JsonObject jsonObject = (JsonObject) obj;

      long tweet_id = jsonObject.get("id").getAsLong();
      String created_at = jsonObject.get("created_at").getAsString();
      String text = jsonObject.get("text").getAsString();
      long user_id = jsonObject.getAsJsonObject("user").get("id").getAsLong();
      int retweet_count = jsonObject.get("retweet_count").getAsInt();

      ArrayList<String> hashtags = new ArrayList<>();
      JsonArray jsonHashtags = jsonObject.getAsJsonObject("entities").getAsJsonArray("hashtags");
      for (int i = 0; i < jsonHashtags.size(); i++) {
        hashtags.add(jsonHashtags.get(i).getAsJsonObject().get("text").getAsString());
      }

      Tweet tweet = new Tweet(created_at, text, user_id, retweet_count, hashtags);

      context.write(new LongWritable(tweet_id), tweet);
    }

  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "Parse Tweet");
    job.setJarByClass(TweetParser.class);
    job.setMapperClass(TPMapper.class);
    job.setNumReduceTasks(0);
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
