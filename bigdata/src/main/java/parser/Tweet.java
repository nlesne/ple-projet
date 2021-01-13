package parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.hadoop.io.Writable;

public class Tweet implements Writable, Cloneable {

  String created_at;
  String text;
  long user_id;
  int retweet_count;
  ArrayList<String> hashtags;

  public Tweet() {
  }

  public Tweet(String created_at, String text, long user_id, int retweet_count, ArrayList<String> hashtags ) {
    this.created_at = new String(created_at);
    this.text = text;
    this.user_id = user_id;
    this.retweet_count = retweet_count;
    this.hashtags = new ArrayList<>(hashtags);
  }

  public Tweet clone() {
    try {
      return (Tweet) super.clone();
    } catch (Exception e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
      System.exit(-1);
    }
    return null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBytes(created_at);
    out.writeBytes(text);
    out.writeLong(user_id);
    out.writeInt(retweet_count);
    out.writeInt(hashtags.size());
    for (String hashtag : hashtags) {
      out.writeBytes(hashtag);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    created_at = in.readLine();
    text = in.readLine();
    user_id = in.readLong();
    retweet_count = in.readInt();
    int hashtagCount = in.readInt();
    hashtags = new ArrayList<>();
    for (int i = 0; i < hashtagCount; i++) {
      hashtags.add(in.readLine());
    }

  }

  public String toString() {
    return created_at
        + ";"
        + text
        + ";"
        + "user_id : " + user_id
        + ";"
        + "retweets : " + retweet_count
        + ";"
        + "hashtags : " + hashtags.toString()
        ;
  }
}
