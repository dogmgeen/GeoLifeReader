import java.io.FileNotFoundException;
import java.text.ParseException;


public abstract class GeoLifeEntity {
	protected double maxX;
	protected double maxY;
	protected double minX;
	protected double minY;
	protected double minTime;
	protected double maxTime;
	protected Histogram timeDeltaHistogram;
	
	public abstract void performAnalysis() throws FileNotFoundException, ParseException;
	public abstract Histogram getTimeDeltaHistogram();
	
	public GeoLifeEntity() {
		minX = minY = Double.MAX_VALUE;	// All values are smaller than Double.MAX_VALUE.
		maxX = maxY = Double.MIN_VALUE; // All values are bigger than Double.MIN_VALUE.
		minTime = Integer.MAX_VALUE;
		maxTime = Integer.MIN_VALUE;
		
		timeDeltaHistogram = new Histogram();
	}
	
	public void printSummary() {
		System.out.printf("Min point: (%f, %f)\n", minX, minY);
		System.out.printf("Max point: (%f, %f)\n", maxX, maxY);
		System.out.printf("Map dimensions (meters): (%f, %f)\n", (maxX - minX), (maxY - minY));
		System.out.printf("Min time: %f seconds\n", minTime);
		System.out.printf("Max time: %f seconds\n", maxTime);
		System.out.printf("Time duration: %f seconds\n", maxTime - minTime);
		System.out.printf("               %f hours\n", (maxTime - minTime)/(3600));
		System.out.printf("               %f days\n", (maxTime - minTime)/(3600*24));
		System.out.printf("               %f months\n", (maxTime - minTime)/(3600*24*30));
		System.out.printf("               %f years\n", (maxTime - minTime)/(3600*24*30*12));
		System.out.println("====================================================");
	}
	
	protected void updateMinMaxData(GeoLifeEntity u) {
    maxX = Math.max(u.getMaxX(), maxX);
    minX = Math.min(u.getMinX(), minX);
    maxY = Math.max(u.getMaxY(), maxY);
    minY = Math.min(u.getMinY(), minY);
    maxTime = Math.max(u.getMaxTime(), maxTime);
    minTime = Math.min(u.getMinTime(), minTime);
	}
	
	public double getMaxX() {
		return maxX;
	}
	
	public void setMaxX(double maxX) {
		this.maxX = maxX;
	}
	
	public double getMaxY() {
		return maxY;
	}
	
	public void setMaxY(double maxY) {
		this.maxY = maxY;
	}
	
	public double getMinX() {
		return minX;
	}
	
	public void setMinX(double minX) {
		this.minX = minX;
	}
	
	public double getMinY() {
		return minY;
	}
	
	public void setMinY(double minY) {
		this.minY = minY;
	}
	
	public double getMinTime() {
		return minTime;
	}
	
	public void setMinTime(double minTime) {
		this.minTime = minTime;
	}
	
	public double getMaxTime() {
		return maxTime;
	}
	
	public void setMaxTime(double maxTime) {
		this.maxTime = maxTime;
	}
}
