import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Histogram extends HashMap<Integer, Integer> {

	private static final long serialVersionUID = -1610144766934801447L;

	public void writeToFile(String string) throws IOException {
		FileWriter fstream = new FileWriter(string, false);
		BufferedWriter out = new BufferedWriter(fstream);
	    for (Map.Entry<Integer, Integer> entry : entrySet()) {
	    	Integer key = entry.getKey();
		    Integer value = entry.getValue();
		    out.write(key.toString() + "," + value.toString() + "\n");
		}
		out.close();
	}

	public void update(Histogram h) {
		for (Map.Entry<Integer, Integer> entry : h.entrySet()) {
			Integer key = entry.getKey();
		    Integer value = entry.getValue();
		    
		    if (containsKey(key)) {
		    	put(key, get(key) + value);
		    } else {
		    	put(key, value);
		    }
		}	
	}

	public void increment(int i) {
		if (containsKey(i)) {
	    	put(i, get(i) + 1);
	    } else {
	    	put(i, 1);
	    }
	}
}
