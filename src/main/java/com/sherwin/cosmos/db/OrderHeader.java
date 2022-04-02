package com.sherwin.cosmos.db;

public class OrderHeader {
    private String id="";
    private String order_id="";
    private String store="";
    private String status="";
    private String status_date="";
    private String origin="";
    private String pos_control_number="";
    private String origin_date="";
    private String order_create_employee="";
    private String customer_id="";
    private String customer_contact="";
    private String currency="";
    private String total_order_lines="";
    private String ship_given_name="";
    private String ship_address1="";
    private String ship_city="";
    private String ship_state="";
    private String ship_postal="";
    private String billing_given_name="";
    private String billing_address2="";
    private String billing_city="";
    private String billing_state="";
    private String billing_postal="";
    private String contact_phone="";
    private String order_required_date="";
    private String src_system_load_date="";
    private String pos_ord_hdr_pk_guid="";
    private String pos_ord_hdr_xmit_guid="";
    private String pos_ord_hdr_version_guid="";
    private String source_location="";
    private String run_cycle="";

    public OrderHeader() {
    }
    public String getId() {
		return this.id;
	}

    public void set(String id) {
		this.id = id;
	}

    public String getOrder_id() {
        return this.order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getStore() {
        return this.store;
    }

    public void setStore(String store) {
        this.store = store;
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

    public String getOrigin() {
        return this.origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getPos_control_number() {
        return this.pos_control_number;
    }

    public void setPos_control_number(String pos_control_number) {
        this.pos_control_number = pos_control_number;
    }

    public String getOrigin_date() {
        return this.origin_date;
    }

    public void setOrigin_date(String origin_date) {
        this.origin_date = origin_date;
    }

    public String getOrder_create_employee() {
        return this.order_create_employee;
    }

    public void setOrder_create_employee(String order_create_employee) {
        this.order_create_employee = order_create_employee;
    }

    public String getCustomer_id() {
        return this.customer_id;
    }

    public void setCustomer_id(String customer_id) {
        this.customer_id = customer_id;
    }

    public String getCustomer_contact() {
        return this.customer_contact;
    }

    public void setCustomer_contact(String customer_contact) {
        this.customer_contact = customer_contact;
    }

    public String getCurrency() {
        return this.currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getTotal_order_lines() {
        return this.total_order_lines;
    }

    public void setTotal_order_lines(String total_order_lines) {
        this.total_order_lines = total_order_lines;
    }

    public String getShip_given_name() {
        return this.ship_given_name;
    }

    public void setShip_given_name(String ship_given_name) {
        this.ship_given_name = ship_given_name;
    }

    public String getShip_address1() {
        return this.ship_address1;
    }

    public void setShip_address1(String ship_address1) {
        this.ship_address1 = ship_address1;
    }

    public String getShip_city() {
        return this.ship_city;
    }

    public void setShip_city(String ship_city) {
        this.ship_city = ship_city;
    }

    public String getShip_state() {
        return this.ship_state;
    }

    public void setShip_state(String ship_state) {
        this.ship_state = ship_state;
    }

    public String getShip_postal() {
        return this.ship_postal;
    }

    public void setShip_postal(String ship_postal) {
        this.ship_postal = ship_postal;
    }

    public String getBilling_given_name() {
        return this.billing_given_name;
    }

    public void setBilling_given_name(String billing_given_name) {
        this.billing_given_name = billing_given_name;
    }

    public String getBilling_address2() {
        return this.billing_address2;
    }

    public void setBilling_address2(String billing_address2) {
        this.billing_address2 = billing_address2;
    }

    public String getBilling_city() {
        return this.billing_city;
    }

    public void setBilling_city(String billing_city) {
        this.billing_city = billing_city;
    }

    public String getBilling_state() {
        return this.billing_state;
    }

    public void setBilling_state(String billing_state) {
        this.billing_state = billing_state;
    }

    public String getBilling_postal() {
        return this.billing_postal;
    }

    public void setBilling_postal(String billing_postal) {
        this.billing_postal = billing_postal;
    }

    public String getContact_phone() {
        return this.contact_phone;
    }

    public void setContact_phone(String contact_phone) {
        this.contact_phone = contact_phone;
    }

    public String getOrder_required_date() {
        return this.order_required_date;
    }

    public void setOrder_required_date(String order_required_date) {
        this.order_required_date = order_required_date;
    }

    public String getSrc_system_load_date() {
        return this.src_system_load_date;
    }

    public void setSrc_system_load_date(String src_system_load_date) {
        this.src_system_load_date = src_system_load_date;
    }

    public String getPos_ord_hdr_pk_guid() {
        return this.pos_ord_hdr_pk_guid;
    }

    public void setPos_ord_hdr_pk_guid(String pos_ord_hdr_pk_guid) {
        this.pos_ord_hdr_pk_guid = pos_ord_hdr_pk_guid;
    }

    public String getPos_ord_hdr_xmit_guid() {
        return this.pos_ord_hdr_xmit_guid;
    }

    public void setPos_ord_hdr_xmit_guid(String pos_ord_hdr_xmit_guid) {
        this.pos_ord_hdr_xmit_guid = pos_ord_hdr_xmit_guid;
    }

    public String getPos_ord_hdr_version_guid() {
        return this.pos_ord_hdr_version_guid;
    }

    public void setPos_ord_hdr_version_guid(String pos_ord_hdr_version_guid) {
        this.pos_ord_hdr_version_guid = pos_ord_hdr_version_guid;
    }

    public String getSource_location() {
        return this.source_location;
    }

    public void setSource_location(String source_location) {
        this.source_location = source_location;
    }

    public String getRun_cycle() {
        return this.run_cycle;
    }

    public void setRun_cycle(String run_cycle) {
        this.run_cycle = run_cycle;
    }

	


    
}
