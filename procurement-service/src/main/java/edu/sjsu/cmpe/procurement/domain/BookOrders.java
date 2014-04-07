package edu.sjsu.cmpe.procurement.domain;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BookOrders {
	@JsonProperty
	private String id = "78033";

	@JsonProperty
	private  List<Integer> order_book_isbns = new ArrayList<Integer>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Integer> getOrder_book_isbns() {
		return order_book_isbns;
	}

	public void setOrder_book_isbns(List<Integer> order_book_isbns) {
		this.order_book_isbns = order_book_isbns;
	}
	
}
