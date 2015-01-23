import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.text.ParseException;
import java.util.Scanner;


public class GeoLifeFile extends GeoLifeEntity {
	private File f;

	public GeoLifeFile(URI uri) {
		f = new File(uri);
		
		minX = minY = Double.MAX_VALUE;	// All values are smaller than Double.MAX_VALUE.
		maxX = maxY = Double.MIN_VALUE; // All values are bigger than Double.MIN_VALUE.
		minTime = Integer.MAX_VALUE;
		maxTime = Integer.MIN_VALUE;
	}

	@Override
	public void performAnalysis() throws FileNotFoundException, ParseException {
		// create a reader for the first file.
		Scanner s = new Scanner(f.getAbsoluteFile());
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
		
		printSummary();
		s.close();
		
	}

	public String getName() {
		return f.getName();
	}

	public File getFile() {
		return f;
	}

}
