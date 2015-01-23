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
		
		System.out.println("######################################################");
		System.out.println("Reader: Finished determining min-max data");
		
		System.out.println("Statistics for GeoLife simulation");
		System.out.printf("Min point: (%f, %f)\n", r.minX, r.minY);
		System.out.printf("Max point: (%f, %f)\n", r.maxX, r.maxY);
		System.out.printf("Map dimensions (meters): (%f, %f)\n", (r.maxX - r.minX), (r.maxY - r.minY));
		System.out.printf("Min time: %f seconds\n", r.minTime);
		System.out.printf("Max time: %f seconds\n", r.maxTime);
		System.out.printf("Time duration: %f seconds\n", r.maxTime - r.minTime);
		System.out.printf("               %f hours\n", (r.maxTime - r.minTime)/(3600));
		System.out.printf("               %f days\n", (r.maxTime - r.minTime)/(3600*24));
		System.out.printf("               %f months\n", (r.maxTime - r.minTime)/(3600*24*30));
		System.out.printf("               %f years\n", (r.maxTime - r.minTime)/(3600*24*30*12));
		System.out.println("====================================================");
	}

}
