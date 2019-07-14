package com.google.bigquery;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMain {

	@Test
	public void testHelloWorld() {
		assertEquals("Hello World!!!", Main.helloworld());
	}

	@Test
	public void testGetHashCode(){
		String q1 = "select * from table where a='b'";
		String q2 = "select * from table where a='c'";
		assertEquals(Main.getHash(q1), Main.getHash(q2));

	}
	@Test
	public void testGetHashCode2(){
		String q1="select * from table where a=1";
		String q2="select * from table where a=2";
		assertEquals(Main.getHash(q1), Main.getHash(q2));

	}
	@Test
	public void testBQ() {
		try {
			System.out.println(Main.runBQ());
		} catch (Exception e){

		}
		
	}
}
