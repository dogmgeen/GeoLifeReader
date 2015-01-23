

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class GeoLifeRecord {
	public double x;
	public double y;
	public int t;
	
	private final DateFormat fmt = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
	//private final Pattern SKIP_UNUSED_TOKENS = Pattern.compile(",0,-?\\d+,\\d+(\\.\\d+)?");

	@SuppressWarnings("resource")
	public GeoLifeRecord(String s) throws ParseException {
		Scanner lineScan = new Scanner(s);
		try {
			// lat       long       0 alt days_since_1900  date       time
			// 39.984702,116.318417,0,492,39744.1201851852,2008-10-23,02:53:04
			lineScan.useDelimiter(",");
			
			// Convert GPS latlong to meters.
			x = lineScan.nextDouble();
			y = lineScan.nextDouble();
			
			// skip unused third parameter, altitude, and days since 1900
			//lineScan.skip(SKIP_UNUSED_TOKENS);
			lineScan.next();
			lineScan.next();
			lineScan.next();
			
			String date = lineScan.next();
			String time = lineScan.next();
			
			// Extract time as seconds since 1970.
			t = (int) (fmt.parse(date + " " + time).getTime()/1000L);
		} catch (ParseException e) {
			System.out.println("#################################################");
			System.out.printf( "Critical error parsing %s\n", s);
			System.out.println("#################################################");
			throw e;
		}
		
		lineScan.close();
	}
}