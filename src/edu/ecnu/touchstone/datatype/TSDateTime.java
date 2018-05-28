package edu.ecnu.touchstone.datatype;

import java.text.SimpleDateFormat;

// in some databases such as MySQL, the arithmetical calculation of 'DateTime' typed attribute 
// is based on the number represented by original string
public class TSDateTime extends TSDate {
	
	private static final long serialVersionUID = 1L;

	public TSDateTime() {
		super();
	}

	public TSDateTime(float nullRatio, String startTime, String endTime) {
		super(nullRatio, startTime, endTime);
	}
	
	public TSDateTime(TSDateTime tsDateTime) {
		super();
		init();
		this.nullRatio = tsDateTime.nullRatio;
		this.startTimeMilliseconds = tsDateTime.startTimeMilliseconds;
		this.endTimeMilliseconds = tsDateTime.endTimeMilliseconds;
		this.startTimeOriginalString = tsDateTime.startTimeOriginalString;
		this.endTimeOriginalString = tsDateTime.endTimeOriginalString;
	}
	
	@Override
	protected void init() {
		sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
		defaultStartTime = "1900-01-01-00:00:00";
		defaultEndTime = "2000-12-31-23:59:59";
	}

	@Override
	public String toString() {
		return "TSDateTime [nullRatio=" + nullRatio + ", startTimeMilliseconds=" + startTimeMilliseconds
				+ ", endTimeMilliseconds=" + endTimeMilliseconds + ", startTimeOriginalString="
				+ startTimeOriginalString + ", endTimeOriginalString=" + endTimeOriginalString + "]";
	}
}
