package com.speckvonschmeck.models;

import java.util.LinkedHashMap;

public class Meta extends LinkedHashMap<String, String>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	String title, scans, pepmass, charge, rtInSeconds;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getScans() {
		return scans;
	}

	public void setScans(String scans) {
		this.scans = scans;
	}

	public String getPepmass() {
		return pepmass;
	}

	public void setPepmass(String pepmass) {
		this.pepmass = pepmass;
	}

	public String getCharge() {
		return charge;
	}

	public void setCharge(String charge) {
		this.charge = charge;
	}

	public String getRtInSeconds() {
		return rtInSeconds;
	}

	public void setRtInSeconds(String rtInSeconds) {
		this.rtInSeconds = rtInSeconds;
	}
	
	

}
