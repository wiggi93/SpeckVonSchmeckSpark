package com.speckvonschmeck.models;

import java.util.List;

public class Spectrum2 {
	Meta meta;
	List<Integer> x;
	List<Integer> y;
	
	public Meta getMeta() {
		return meta;
	}
	
	public void setMeta(Meta meta) {
		this.meta = meta;
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


	@Override
	public String toString() {
		return "Spectrum [meta=" + meta + ", data=" + x + "  "+ y + "]";
	}
	
}
