package com.autohome.pvcount.utils;

import java.io.Serializable;
@SuppressWarnings("all")
public class ThreadS extends Thread implements Serializable{
	
	public ThreadS(Runnable runa){
		super(runa);
	}
	public ThreadS(){
		
	};
}
