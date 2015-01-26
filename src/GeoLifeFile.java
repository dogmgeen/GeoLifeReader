import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.text.ParseException;
import java.util.Scanner;


public class GeoLifeFile extends GeoLifeEntity {
	private File f;
  private GeoLifeRecord first;
  private GeoLifeRecord last;

	public GeoLifeFile(URI uri) {
		super();
		f = new File(uri);
    first = null;
    last = null;
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
		Integer last_time = null;
    int timeDelta = 0;
    String line = null;
		while (s.hasNext()) {
			//39.984702,116.318417,0,492,39744.1201851852,2008-10-23,02:53:04
      line = s.nextLine();
			record = new GeoLifeRecord(line);

      if (first == null) {
        first = record;
      }
			
			if (last_time != null) {
        timeDelta = record.t - last_time;
				timeDeltaHistogram.increment(timeDelta);
        if (timeDelta < 0 || timeDelta > 5933) {
          System.out.printf(
            "Odd time delta in file %s (%d seconds)\n",
            getName(), timeDelta
          );
          System.out.println(line);
        }

			}
			
			minX = Math.min(record.x, minX);
			maxX = Math.max(record.x, maxX);
			minY = Math.min(record.y, minY);
			maxY = Math.max(record.y, maxY);
			minTime = Math.min(record.t, minTime);
			maxTime = Math.max(record.t, maxTime);
			last_time = record.t;
		}

    last = record;
		
		//printSummary();
		s.close();
	}

	public String getName() {
		return f.getName();
	}

	public File getFile() {
		return f;
	}

	@Override
	public Histogram getTimeDeltaHistogram() {
		return timeDeltaHistogram;
	}

  public GeoLifeRecord getFirstRecord() {
    return first;
  }

  public GeoLifeRecord getLastRecord() {
    return last;
  }

}
