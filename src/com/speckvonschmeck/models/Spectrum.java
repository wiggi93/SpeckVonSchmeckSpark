package com.speckvonschmeck.models;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Spectrum {
	LinkedHashMap<String, String> meta;
	List<Data> data;
	
	public Spectrum(){
		meta = new LinkedHashMap<String, String>();
		data = new ArrayList<Data>();
	}
	
	public LinkedHashMap<String, String> getMeta() {
		return meta;
	}
	public void setMeta(LinkedHashMap<String, String> meta) {
		this.meta = meta;
	}
	public List<Data> getData() {
		return data;
	}
	public void setData(List<Data> data) {
		this.data = data;
	}
	
}
