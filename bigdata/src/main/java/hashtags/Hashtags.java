package hashtags;

import org.apache.hadoop.util.ProgramDriver;


public class Hashtags {

  public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("k", hashtags.KHashtags.class, "...");
			pgd.addClass("userHashtags", hashtags.UserHashtag.class, "");
			pgd.addClass("count", hashtags.CountHashtags.class, "...");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
