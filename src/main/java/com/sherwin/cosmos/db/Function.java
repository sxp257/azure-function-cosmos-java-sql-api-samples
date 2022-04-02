package com.sherwin.cosmos.db;

import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;



/**
 * Hello function with HTTP Trigger.
 */
public class Function {


    @FunctionName("hello")
    public String hello(@HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) String req,
                        ExecutionContext context) {
        //context.getLogger().info("Function Hello is invoked ...");
        return String.format("Hello, %s!", req);
    }

    @FunctionName("helloHttp")
    public HttpResponseMessage hellohttp(@HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>>  req,
                        ExecutionContext context) {
        
        context.getLogger().info("Java HTTP trigger processed a request.");                    
        // Parse query parameter
        final String query = req.getQueryParameters().get("name");
        final String name = req.getBody().orElse(query);

        if (name == null) {
            return req.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a name on the query string or in the request body").build();
        } else {
            return req.createResponseBuilder(HttpStatus.OK).body("Hello, " + name).build();
        }
    }

    @FunctionName("helloHttpJson")
    public HttpResponseMessage helloHttpJson(@HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>>  req,
                        ExecutionContext context) {
        
        context.getLogger().info("Java HTTP trigger processed a request.");                    
        // Parse query parameter
        final String query = req.getQueryParameters().get("name");
        final String name = req.getBody().orElse(query);

        if (name == null) {
            return req.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a name on the query string or in the request body").build();
        } else {
            return req.createResponseBuilder(HttpStatus.OK).header("content-type", "application/json").body("Hello, " + name).build();
        }
    }
        @FunctionName("logCosmosOrder")
    public HttpResponseMessage execute(@HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>>  req,
                        ExecutionContext context) {
        Order order= null;
        context.getLogger().info("Java HTTP trigger processed a request.");                    
        // Parse query parameter
        final String query = req.getQueryParameters().get("order_id");
        final String orderId = req.getBody().orElse(query);
        QueriesQuickStartOrderLine p = new QueriesQuickStartOrderLine();
        try {
            context.getLogger().info("Starting SYNC main");
            order = p.queriesDemo(orderId);
            context.getLogger().info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().severe(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            context.getLogger().info("Closing the client");
            p.shutdown();
        }

        if (orderId == null) {
            return req
                    .createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Please pass a orderId on the query string or in the request body")
                    .build();
        } else {
            if (!("".equals(order.getOrderHeader().getOrder_id()))){
                Gson gsonLine = new GsonBuilder().setPrettyPrinting().create();
                String jsonLine = gsonLine.toJson(order);
                return req
                    .createResponseBuilder(HttpStatus.OK)
                    .header("content-type", "application/json")
                    .body(jsonLine)
                    .build();
            } else {
                return req
                    .createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("Order not found")
                    .build();
            }
            
        }
    }

}
