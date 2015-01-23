
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class GeoLifeGPSTrajectoriesReader {
	/** external directory containing GeoLife PLT files -setting id ({@value})*/
	Map<String, GeoLifeUser> users;
	public double maxX;
	public double maxY;
	public double minX;
	public double minY;
	public double minTime;
	public double maxTime;

	public GeoLifeGPSTrajectoriesReader(String directory) throws IOException, ParseException {
		
		// Search GeoLife files for initial locations of participating nodes
		Path directoryUrl = Paths.get(directory);
		// Determine the directory that contains each of the user trajectories.
		
		// 1. Find plt file. Once found, stop directory search.
		PltFinder finder = new PltFinder();
		System.out.printf("Searching for PLT files within %s\n", directory);
        Files.walkFileTree(directoryUrl, finder);
        Path samplePltFile = finder.get();
        System.out.printf("PLT file found at %s\n", samplePltFile.toAbsolutePath().toString());
        
		// 2. Extract all trajectory data directory from plt file URI.
        Path pathToAllFiles = samplePltFile.getParent().getParent().getParent();
        
		// 3. From data directory, enumerate users.
        //  http://stackoverflow.com/a/5125258
        System.out.println("Enumerating users...");
        String[] directories = pathToAllFiles.toFile()
        						.list(new FilenameFilter() {
          @Override
          public boolean accept(File current, String name) {
            return new File(current, name).isDirectory();
          }
        });
        
        users = new HashMap<String, GeoLifeUser>(directories.length);
        for (String userID: directories) {    		
        	GeoLifeUser u = new GeoLifeUser(pathToAllFiles, userID);
        	users.put(u.getID(), u);
        }
        
        maxX = Double.MIN_VALUE;
		maxY = Double.MIN_VALUE;
		minX = Double.MAX_VALUE;
		minY = Double.MAX_VALUE;
		minTime = Double.MAX_VALUE;
		maxTime = Double.MIN_VALUE;
		
		System.out.println("User enumeration complete");
		System.out.println("=========================");
	}

	public void performAnalysis() throws FileNotFoundException, ParseException {
		for(Entry<String, GeoLifeUser> userEntry: users.entrySet()){
			GeoLifeUser u = userEntry.getValue();
        	System.out.println("User " + u.getID());
        	u.performAnalysis();
        	
        	// 4. For each user, associate a list of plt file URIs, sorted by time.
    		// 5. For each user, create a reader for the first file.
    		// 6. For each user, assign:
        	
    		//  localMaxX: iterate through all files (ugh... need some preprocessing)
    		//  maxX: max over all users
        	maxX = Math.max(u.getMaxX(), maxX);
        	
        	//  localMinX: iterate through all files (^)
    		//  minX: min over all users
        	minX = Math.min(u.getMinX(), minX);

        	//  localMaxY: iterate through all files (^)
    		//  maxY: max over all users
        	maxY = Math.max(u.getMaxY(), maxY);
        	
        	//  localMinY: iterate through all files (^)
        	//	minY: max over all users
        	minY = Math.min(u.getMinY(), minY);
        	
    		//  maxTime: max of all localMaxTimes
        	maxTime = Math.max(u.getMaxTime(), maxTime);
        	
    		//  localMinTime: First line of first plt file
    		//  minTime: min of all localMinTimes
        	minTime = Math.min(u.getMinTime(), minTime);
        }
	}
}
