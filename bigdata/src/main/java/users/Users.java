package users;

import org.apache.hadoop.util.ProgramDriver;

public class Users {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("hashtags", AllHashtagUser.class,"");
			pgd.addClass("count", CountTweet.class, "");
			pgd.addClass("country", TweetCountry.class, "");
			pgd.addClass("lang", TweetLanguage.class, "");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
