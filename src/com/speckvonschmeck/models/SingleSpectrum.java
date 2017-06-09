package com.speckvonschmeck.models;

import java.io.Serializable;
import java.util.List;

public class SingleSpectrum implements Serializable{
	String charge, pepmass, rtinseconds, scans, title;
	List<Integer> x;
	List<Integer> y;

	public String getCharge() {
		return charge;
	}
	public void setCharge(String charge) {
		this.charge = charge;
	}
	public String getPepmass() {
		return pepmass;
	}
	public void setPepmass(String pepmass) {
		this.pepmass = pepmass;
	}
	public String getRtinseconds() {
		return rtinseconds;
	}
	public void setRtinseconds(String rtinseconds) {
		this.rtinseconds = rtinseconds;
	}
	public String getScans() {
		return scans;
	}
	public void setScans(String scans) {
		this.scans = scans;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public List<Integer> getX() {
		return x;
	}
	public void setX(List<Integer> x) {
		this.x = x;
	}
	public List<Integer> getY() {
		return y;
	}
	public void setY(List<Integer> y) {
		this.y = y;
	}

}
