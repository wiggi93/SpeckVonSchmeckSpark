package com.speckvonschmeck.spark.models;

public class Data {
	double x, y;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	@Override
	public String toString() {
		return "Data [x=" + x + ", y=" + y + "]";
	}
	
}
