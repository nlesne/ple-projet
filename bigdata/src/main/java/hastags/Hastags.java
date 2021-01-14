package hastags;

import org.apache.hadoop.util.ProgramDriver;


public class Hastags {

  public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("k", hastags.KHastags.class, "...");
			pgd.addClass("count", hastags.CountHastags.class, "...");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
