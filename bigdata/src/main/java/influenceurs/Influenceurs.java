package influenceurs;

import org.apache.hadoop.util.ProgramDriver;

public class Influenceurs {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			//pgd.addClass("", influenceurs.class, "");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
