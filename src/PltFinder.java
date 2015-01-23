import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;

import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class PltFinder extends SimpleFileVisitor<Path> {
	private Path somePltFilePath;

	public Path get() {
		return somePltFilePath;
	}
	
    // Check if each file in the directory tree is a plt file.
	//  If a file is found, save its path and terminate.
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
    	Path name = file.getFileName();
    	System.out.println(file.toAbsolutePath().toString());
        if (name != null && name.toString().endsWith("plt")) {
            somePltFilePath = file;
            return TERMINATE;
        } else {
        	return CONTINUE;
        }
    }
}