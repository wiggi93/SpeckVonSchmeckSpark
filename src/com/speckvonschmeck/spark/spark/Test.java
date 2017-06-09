package com.speckvonschmeck.spark.spark;

import java.io.Serializable;

public class Test implements Serializable{
	int id;
	int x;
	int y;
	
	public Test(int id, int x, int y){
		this.id=id;
		this.x=x;
		this.y=y;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public int getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	
	

}
