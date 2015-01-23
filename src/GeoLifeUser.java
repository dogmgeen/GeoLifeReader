

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class GeoLifeUser {
	private String userID;
	List<File> pltFiles;
	private Scanner plt_scanner;
	private int minTime;
	private int maxTime;
	
	private double minX;
	private double minY;
	private double maxX;
	private double maxY;
	private boolean is_initialized;
	
	public GeoLifeUser(Path root, String userID) throws FileNotFoundException, ParseException {
		this.userID = userID;
		pltFiles = initPltFiles(root);
		
		// Initialize read values for later.
		is_initialized = false;
		
		minX = minY = Double.MAX_VALUE;	// All values are smaller than Double.MAX_VALUE.
		maxX = maxY = Double.MIN_VALUE; // All values are bigger than Double.MIN_VALUE.
		minTime = Integer.MAX_VALUE;
		maxTime = Integer.MIN_VALUE;	
	}
	
	public void performAnalysis() throws FileNotFoundException, ParseException {
		for (File f: pltFiles) {
			System.out.println("User " + userID + " file " + f.getName());
			// create a reader for the first file.
			plt_scanner = new Scanner(f);
			plt_scanner.nextLine();						//Geolife trajectory
			plt_scanner.nextLine();						//WGS 84
			plt_scanner.nextLine();						//Altitude is in Feet
			plt_scanner.nextLine();						//Reserved 3
			plt_scanner.nextLine();						//0,2,255,My Track,0,0,2,8421376
			plt_scanner.nextLine();						//0
			
			GeoLifeRecord record = null;
			while (plt_scanner.hasNext()) {
				//39.984702,116.318417,0,492,39744.1201851852,2008-10-23,02:53:04
				record = new GeoLifeRecord(plt_scanner.nextLine());
				
				minX = Math.min(record.x, minX);
				maxX = Math.max(record.x, maxX);
				minY = Math.min(record.y, minY);
				maxY = Math.max(record.y, maxY);
			}
			// Once all files have been iterated, the remaining value in
			//  epoch time is the max time.
			maxTime = record.t;
			plt_scanner.close();
		}
		
		System.out.println("Statistics for GeoLife user " + userID);
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
	}

	private List<File> initPltFiles(Path root) {
		// Create a path representing where the individual PLT files are located.
		//  It is expected this path will look like:
		//  /some/given/path/Data/<userID>/Trajectory/
		Path dataFilesDirectory = root.resolve(userID).resolve("Trajectory");
		
		// Find all plt filenames inside this directory.
		String[] pltFilenames = dataFilesDirectory.toFile().list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.endsWith("plt");
          }
        });
		
		System.out.printf("%d files found for user %s\n", pltFilenames.length, userID);
		
		// Create a set of Files for each of these files.
		List<File> files = new LinkedList<File>();
		for (String f: pltFilenames) {
			files.add(dataFilesDirectory.resolve(f).toFile());
		}
		
		// Sort files such that the first file is the earliest one created.
		//  The sorting below is accomplished through lexicographical sorting
		//  based on the file paths. Since file names are in the format
		//		20081023025304.plt
		//  with the same length, and with the date format has the most
		//  significant time (year) first and least significant time (seconds)
		//  last, lexicographical ordering preserves temporal ordering.
		Collections.sort(files);
		
		// Oldest file (the earliest one created) is now the first file in
		//  the list.
		//Collections.reverse(files);
		return files;
	}

	public String getID() {
		return userID;
	}

	public double getMaxX() {
		return maxX;
	}

	public double getMinX() {
		return minX;
	}

	public double getMaxY() {
		return maxY;
	}

	public double getMinY() {
		return minY;
	}

	public int getMaxTime() {
		return maxTime;
	}

	public int getMinTime() {
		return minTime;
	}

	public GeoLifeRecord getNextMovement() throws FileNotFoundException, ParseException {
		if (!plt_scanner.hasNext()) {
			plt_scanner.close();
			
			File f = pltFiles.remove(0);
			System.out.printf("Opening new file: " + f.getName());
			plt_scanner = new Scanner(f);
			
			// Skip over first six lines.
			plt_scanner.nextLine();	//Geolife trajectory
			plt_scanner.nextLine();	//WGS 84
			plt_scanner.nextLine();	//Altitude is in Feet
			plt_scanner.nextLine();	//Reserved 3
			plt_scanner.nextLine();	//0,2,255,My Track,0,0,2,8421376
			plt_scanner.nextLine();	//0
		}
		GeoLifeRecord record = new GeoLifeRecord(plt_scanner.nextLine());
		return record;
	}

	public boolean hasAnotherMovement() {
		// If there are more tokens in the current file, then return
		//  true.
		if (plt_scanner.hasNext()) {
			return true;
		}
		
		// If there are no more tokens, then another file must be loaded.
		//  Thus, another file must be available.
		if (pltFiles.isEmpty()) {
			return false;
		}
		
		// At this point, there either is another file, or there are more
		//  tokens in the current file.
		return true;
	}
}