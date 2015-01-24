
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

public class GeoLifeGPSTrajectoriesReader extends GeoLifeEntity {
	/** external directory containing GeoLife PLT files -setting id ({@value})*/
	Map<String, GeoLifeUser> users;

	public GeoLifeGPSTrajectoriesReader(String directory) throws IOException, ParseException {
		super();
		
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
		
		System.out.println("User enumeration complete");
		System.out.println("=========================");
	}

	public void performAnalysis() throws FileNotFoundException, ParseException {
		for(Entry<String, GeoLifeUser> userEntry: users.entrySet()){
			GeoLifeUser u = userEntry.getValue();
        	System.out.println("User " + u.getID());
        	u.performAnalysis();
        	
        	updateMinMaxData(u);
        }
	}

	@Override
	public Histogram getTimeDeltaHistogram() {
		for(Entry<String, GeoLifeUser> userEntry: users.entrySet()){
			GeoLifeUser u = userEntry.getValue();
			timeDeltaHistogram.update(u.getTimeDeltaHistogram());
        }
		return timeDeltaHistogram;
	}
}
