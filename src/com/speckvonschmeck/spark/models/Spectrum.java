package com.speckvonschmeck.spark.models;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class Spectrum implements Serializable {
	String title, scans, pepmass, charge, rtinseconds;
	UUID uuid;
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
	public UUID getUuid() {
		return uuid;
	}
	public void setUuid(UUID uuid) {
		this.uuid = uuid;
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
	public String getrtinseconds() {
		return rtinseconds;
	}
	public void setrtinseconds(String rtinseconds) {
		this.rtinseconds = rtinseconds;
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
				+ ", rtinseconds=" + rtinseconds + ", uuid=" + uuid + ", x=" + x + ", y=" + y + "]";
	}
	
}
