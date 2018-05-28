package edu.ecnu.touchstone.datatype;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// in some databases such as MySQL, the arithmetical calculation of 'Date' typed attribute 
// is based on the number represented by original string
public class TSDate implements TSDataTypeInfo {

	private static final long serialVersionUID = 1L;
	
	protected float nullRatio;
	protected long startTimeMilliseconds;
	protected long endTimeMilliseconds;
	protected long startTimeOriginalString;
	protected long endTimeOriginalString;
	
	protected SimpleDateFormat sdf = null;
	protected String defaultStartTime = null;
	protected String defaultEndTime = null;
	
	public TSDate() {
		super();
		init();
		nullRatio = 0;
		try {
			startTimeMilliseconds = sdf.parse(defaultStartTime).getTime();
			endTimeMilliseconds = sdf.parse(defaultEndTime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		startTimeOriginalString = Long.parseLong(defaultStartTime.replaceAll("[- :]+", ""));
		endTimeOriginalString = Long.parseLong(defaultEndTime.replaceAll("[- :]+", ""));
	}

	public TSDate(float nullRatio, String startTime, String endTime) {
		super();
		init();
		this.nullRatio = nullRatio;
		try {
			startTimeMilliseconds = sdf.parse(startTime).getTime();
			endTimeMilliseconds = sdf.parse(endTime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		startTimeOriginalString = Long.parseLong(startTime.replaceAll("[- :]+", ""));
		endTimeOriginalString = Long.parseLong(endTime.replaceAll("[- :]+", ""));
	}
	
	public TSDate(TSDate tsDate) {
		super();
		init();
		this.nullRatio = tsDate.nullRatio;
		this.startTimeMilliseconds = tsDate.startTimeMilliseconds;
		this.endTimeMilliseconds = tsDate.endTimeMilliseconds;
		this.startTimeOriginalString = tsDate.startTimeOriginalString;
		this.endTimeOriginalString = tsDate.endTimeOriginalString;
		
	}

	protected void init() {
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		defaultStartTime = "1900-01-01";
		defaultEndTime = "2000-12-31";
	}
	
	@Override
	public Long geneData() {
		if (Math.random() < nullRatio) {
			return null;
		}
		return (long)(Math.random() * (endTimeMilliseconds - startTimeMilliseconds + 1) + startTimeMilliseconds);
	}

	@Override
	public String toString() {
		return "TSDate [nullRatio=" + nullRatio + ", startTimeMilliseconds=" + startTimeMilliseconds
				+ ", endTimeMilliseconds=" + endTimeMilliseconds + ", startTimeOriginalString="
				+ startTimeOriginalString + ", endTimeOriginalString=" + endTimeOriginalString + "]";
	}

	@Override
	public double getMinValue() {
		return startTimeMilliseconds;
	}

	@Override
	public double getMaxValue() {
		return endTimeMilliseconds;
	}
}
