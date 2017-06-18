package com.speckvonschmeck.spark.models;

import java.io.Serializable;
import java.util.UUID;

public class SpecCompare implements Serializable{
	private static final long serialVersionUid = -2178752071460666906L;
	
	UUID uuid, spectrum1id, spectrum2id;
	double score1, score2, score3;
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
	public double getScore1() {
		return score1;
	}
	public void setScore1(double score) {
		this.score1 = score;
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
	public double getScore2() {
		return score2;
	}
	public void setScore2(double score2) {
		this.score2 = score2;
	}
	
	public double getScore3() {
		return score3;
	}
	public void setScore3(double score3) {
		this.score3 = score3;
	}
	@Override
	public String toString() {
		return "SpecCompare [uuid=" + uuid + ", spectrum1id=" + spectrum1id + ", spectrum2id=" + spectrum2id
				+ ", score1=" + score1 + ", score2=" + score2 + ", score3=" + score3 + ", user=" + user + ", time="
				+ time + "]";
	}
	
}
