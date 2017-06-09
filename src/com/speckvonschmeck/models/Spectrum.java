package com.speckvonschmeck.models;

import java.util.List;

public class Spectrum {
	Meta meta;
	List<Data> data;
	
	
	public Meta getMeta() {
		return meta;
	}
	
	public void setMeta(Meta meta) {
		this.meta = meta;
	}
	
	public List<Data> getData() {
		return data;
	}
	
	public void setData(List<Data> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "Spectrum [meta=" + meta + ", data=" + data + "]";
	}
	
}
