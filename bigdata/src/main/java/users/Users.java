package users;

import org.apache.hadoop.util.ProgramDriver;

public class Users {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("countTweets", CountTweet.class, "");
			pgd.addClass("tweetCountry", TweetCountry.class, "");
			pgd.addClass("tweetLang", TweetLanguage.class, "");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
