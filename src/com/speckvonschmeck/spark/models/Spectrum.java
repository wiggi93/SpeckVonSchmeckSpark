package com.speckvonschmeck.spark.models;

import java.util.List;

public class Spectrum {
	String title, scans, pepmass, charge, rtInSeconds;
	List<Double> x;
	List<Double> y;
	
	
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
	public List<Double> getX() {
		return x;
	}
	public void setX(List<Double> x) {
		this.x = x;
	}
	public List<Double> getY() {
		return y;
	}
	public void setY(List<Double> y) {
		this.y = y;
	}
	
	@Override
	public String toString() {
		return "Spectrum [title=" + title + ", scans=" + scans + ", pepmass=" + pepmass + ", charge=" + charge
				+ ", rtInSeconds=" + rtInSeconds + ", x=" + x + ", y=" + y + "]";
	}
	
}
