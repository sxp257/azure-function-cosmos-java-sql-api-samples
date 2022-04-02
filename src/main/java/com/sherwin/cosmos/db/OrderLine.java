package com.sherwin.cosmos.db;

public class OrderLine {
    private String id = "";
    private String line_id = "";
    private String order_id = "";
    private String line_number = "";
    private String lineUOM = "";
    private String pos_line_sales_number = "";
    private String status = "";
    private String status_date = "";
    private String quantity = "";
    private String unit_cost = "";
    private String promise_delivery_date = "";
    private String required_delivery_date = "";
    private String store = "";
    private String pos_ord_line_pk_guid = "";
    private String pos_ord_line_xmit_guid = "";
    private String pos_ord_line_version_guid = "";
    private String run_cycle = "";

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLine_id() {
        return this.line_id;
    }

    public void setLine_id(String line_id) {
        this.line_id = line_id;
    }

    public String getOrder_id() {
        return this.order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getLine_number() {
        return this.line_number;
    }

    public void setLine_number(String line_number) {
        this.line_number = line_number;
    }

    public String getLineUOM() {
        return this.lineUOM;
    }

    public void setLineUOM(String lineUOM) {
        this.lineUOM = lineUOM;
    }

    public String getPos_line_sales_number() {
        return this.pos_line_sales_number;
    }

    public void setPos_line_sales_number(String pos_line_sales_number) {
        this.pos_line_sales_number = pos_line_sales_number;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus_date() {
        return this.status_date;
    }

    public void setStatus_date(String status_date) {
        this.status_date = status_date;
    }

    public String getQuantity() {
        return this.quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getUnit_cost() {
        return this.unit_cost;
    }

    public void setUnit_cost(String unit_cost) {
        this.unit_cost = unit_cost;
    }

    public String getPromise_delivery_date() {
        return this.promise_delivery_date;
    }

    public void setPromise_delivery_date(String promise_delivery_date) {
        this.promise_delivery_date = promise_delivery_date;
    }

    public String getRequired_delivery_date() {
        return this.required_delivery_date;
    }

    public void setRequired_delivery_date(String required_delivery_date) {
        this.required_delivery_date = required_delivery_date;
    }

    public String getStore() {
        return this.store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public String getPos_ord_line_pk_guid() {
        return this.pos_ord_line_pk_guid;
    }

    public void setPos_ord_line_pk_guid(String pos_ord_line_pk_guid) {
        this.pos_ord_line_pk_guid = pos_ord_line_pk_guid;
    }

    public String getPos_ord_line_xmit_guid() {
        return this.pos_ord_line_xmit_guid;
    }

    public void setPos_ord_line_xmit_guid(String pos_ord_line_xmit_guid) {
        this.pos_ord_line_xmit_guid = pos_ord_line_xmit_guid;
    }

    public String getPos_ord_line_version_guid() {
        return this.pos_ord_line_version_guid;
    }

    public void setPos_ord_line_version_guid(String pos_ord_line_version_guid) {
        this.pos_ord_line_version_guid = pos_ord_line_version_guid;
    }

    public String getRun_cycle() {
        return this.run_cycle;
    }

    public void setRun_cycle(String run_cycle) {
        this.run_cycle = run_cycle;
    }

}
