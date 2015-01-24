import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.text.ParseException;

public class Driver {

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		long start = System.currentTimeMillis();
		System.out.printf("TOTAL EXECUTION TIME: %d seconds\n", start);
		
		String inputDirectory = null;
		
		// Check how many arguments were passed in
		if(args.length == 0)
		{
			inputDirectory = "/home/djmvfb/Geolife Trajectories/";
		} else {
			inputDirectory = args[0];
		}
		
		GeoLifeGPSTrajectoriesReader r = null;
		
		try {
			r = new GeoLifeGPSTrajectoriesReader(inputDirectory);
		} catch (NoSuchFileException e) {
			e.printStackTrace();
			System.out.println("\nNo PLT files were found anywhere in the file system tree rooted at:");
			System.out.println(inputDirectory);
			System.out.println("\nAborting operation.\n");
			System.exit(1);
		}
		
		r.performAnalysis();
		r.printSummary();
		
		Histogram h = r.getTimeDeltaHistogram();
		h.writeToFile("time_deltas.csv");
		
		long duration = (System.currentTimeMillis() - start)/1000;
		System.out.printf("TOTAL EXECUTION TIME: %d seconds\n", duration);
		System.out.printf("                      %d minutes\n", duration/60);
		System.out.printf("                      %d hours\n", duration/3600);
	}

}
