import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.text.ParseException;
import java.util.Scanner;


public class GeoLifeFile extends File {
	private static final long serialVersionUID = -2267796368162097230L;
	public double minX;
	public double minY;
	public double maxX;
	public double maxY;
	public int maxTime;
	public int minTime;

	public GeoLifeFile(URI uri) {
		super(uri);
		
		minX = minY = Double.MAX_VALUE;	// All values are smaller than Double.MAX_VALUE.
		maxX = maxY = Double.MIN_VALUE; // All values are bigger than Double.MIN_VALUE.
		minTime = Integer.MAX_VALUE;
		maxTime = Integer.MIN_VALUE;
	}

	public void analyzeFile() throws FileNotFoundException, ParseException {
		// create a reader for the first file.
		Scanner s = new Scanner(this.getAbsoluteFile());
		s.nextLine();						//Geolife trajectory
		s.nextLine();						//WGS 84
		s.nextLine();						//Altitude is in Feet
		s.nextLine();						//Reserved 3
		s.nextLine();						//0,2,255,My Track,0,0,2,8421376
		s.nextLine();						//0
		
		GeoLifeRecord record = null;
		
		while (s.hasNext()) {
			//39.984702,116.318417,0,492,39744.1201851852,2008-10-23,02:53:04
			record = new GeoLifeRecord(s.nextLine());
			
			minX = Math.min(record.x, minX);
			maxX = Math.max(record.x, maxX);
			minY = Math.min(record.y, minY);
			maxY = Math.max(record.y, maxY);
			minTime = Math.min(record.t, minTime);
			maxTime = Math.max(record.t, maxTime);
		}
		
		System.out.printf("Min point: (%f, %f)\n", minX, minY);
		System.out.printf("Max point: (%f, %f)\n", maxX, maxY);
		System.out.printf("Map dimensions (meters): (%f, %f)\n", (maxX-minX), (maxY-minY));
		System.out.printf("Min time: %d seconds\n", minTime);
		System.out.printf("Max time: %d seconds\n", maxTime);
		System.out.printf("Time duration: %d seconds\n", maxTime-minTime);
		System.out.printf("               %d hours\n", (maxTime-minTime)/(3600));
		System.out.printf("               %d days\n", (maxTime-minTime)/(3600*24));
		System.out.printf("               %d months\n", (maxTime-minTime)/(3600*24*30));
		System.out.printf("               %d years\n", (maxTime-minTime)/(3600*24*30*12));
		System.out.println("====================================================");
		
		s.close();
	}

}
