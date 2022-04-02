package com.sherwin.cosmos.db;

import java.util.ArrayList;
import java.util.List;



public class Order {
    private OrderHeader order = new OrderHeader();
    private List<OrderLine> lines = new ArrayList<>();

    public OrderHeader getOrderHeader() {
		return this.order;
	}

    public void setOrderHeader( OrderHeader order) {
		this.order = order;
	}

    public List<OrderLine> getLines()
    {
        return this.lines;
    }

    public void setLines(OrderLine line)
    {
		lines.add(line);
	}

}
