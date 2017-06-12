package com.speckvonschmeck.spark.models;

import java.io.Serializable;
import java.util.UUID;

public class SpecCompare implements Serializable{
	UUID uuid, spectrum1id, spectrum2id;
	double score;
	String user;
	long time;
	public UUID getUuid() {
		return uuid;
	}
	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
	public UUID getSpectrum1id() {
		return spectrum1id;
	}
	public void setSpectrum1id(UUID spectrum1id) {
		this.spectrum1id = spectrum1id;
	}
	public UUID getSpectrum2id() {
		return spectrum2id;
	}
	public void setSpectrum2id(UUID spectrum2id) {
		this.spectrum2id = spectrum2id;
	}
	@Override
	public String toString() {
		return "SpecCompare [uuid=" + uuid + ", spectrum1id=" + spectrum1id + ", spectrum2id=" + spectrum2id
				+ ", score=" + score + ", user=" + user + ", time=" + time + "]";
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
}
