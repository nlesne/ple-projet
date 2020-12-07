import org.apache.hadoop.util.ProgramDriver;

public class TwitterProject {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("hastags", hastags.Hastags.class, "use hastags");
			pgd.addClass("users", users.Users.class, "users");
			pgd.addClass("influenceurs", influenceurs.Influenceurs.class, "influenceurs");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}