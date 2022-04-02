package com.sherwin.cosmos.db;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;

import com.azure.cosmos.models.CosmosQueryRequestOptions;

import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueriesQuickStartOrderLine {

    private CosmosClient client;

    private final String databaseName = "iom";
    private final String headercontainerName = "order_hdr";
    private final String linecontainerName = "order_line";

    private CosmosDatabase database;
    private CosmosContainer hdrContainer;
    private CosmosContainer lineContainer;

    protected static Logger logger = LoggerFactory.getLogger(QueriesQuickStartOrderLine.class);
    private Order order;
    private static final String base_sql = "SELECT * FROM c ";

    public void close() {
        client.close();
    }

    public Order queriesDemo(String orderId) throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        // Create sync client -- directMode
        /*
         * client = new CosmosClientBuilder()
         * .endpoint(AccountSettings.HOST)
         * .key(AccountSettings.MASTER_KEY)
         * .consistencyLevel(ConsistencyLevel.EVENTUAL)
         * .contentResponseOnWriteEnabled(true)
         * .buildClient();
         */

        // Create sync client -- gatewayMode
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .gatewayMode()
                .buildClient();

        getDatabaseIfExists();
        getHeaderContainerIfExists();
        getLineContainerIfExists();

        order = new Order();

        queryDocumentsById(orderId);
        return order;
    }

    private void queryDocumentsById(String orderId) throws Exception {
        logger.info("Query documents by Id.");
        executeQueryOrderHeaderPrintSingleResultById(orderId);
    }

    private void executeQueryOrderHeaderPrintSingleResultById(String orderId) throws Exception {
        String sql = queryEquality(orderId);
        logger.info("Execute query {}", sql);

        CosmosPagedIterable<OrderHeader> filteredOrderHeaders = hdrContainer.queryItems(sql,
                new CosmosQueryRequestOptions(), OrderHeader.class);

        // Print
        if (filteredOrderHeaders.iterator().hasNext()) {
            OrderHeader orderHeader = filteredOrderHeaders.iterator().next();
            // Set Header
            order.setOrderHeader(orderHeader);
            executeQueryOrderLinePrintSingleResult(orderHeader.getOrder_id());
        }

        logger.info("Done.");
    }

    private void executeQueryOrderLinePrintSingleResult(String orderId) throws Exception {
        String sql = queryEquality(orderId);
        logger.info("Execute query {}", sql);

        CosmosPagedIterable<OrderLine> filteredOrderLines = lineContainer.queryItems(sql,
                new CosmosQueryRequestOptions(), OrderLine.class);
        // Set Lines
        filteredOrderLines.forEach(line -> order.setLines(line));
        Gson gsonLine = new GsonBuilder().setPrettyPrinting().create();
        String jsonLine = gsonLine.toJson(order);
        logger.info(String.format("First query result: filteredOrderLine with (/id) = (%s, %s)",
                order.getOrderHeader().getId(), jsonLine));

        logger.info("Done.");
    }

    private String queryEquality(String id) throws Exception {
        logger.info("Query for equality using =");
        return base_sql + "WHERE c.order_id = '" + id + "'";
    }

    private void getHeaderContainerIfExists() throws Exception {
        logger.info("Get container " + headercontainerName + " if  exists.");
        hdrContainer = database.getContainer(headercontainerName);
        logger.info("Done.");
    }

    private void getLineContainerIfExists() throws Exception {
        logger.info("Get container " + linecontainerName + " if  exists.");
        lineContainer = database.getContainer(linecontainerName);
        logger.info("Done.");
    }

    // Database Create
    private void getDatabaseIfExists() throws Exception {
        logger.info("Get database " + databaseName + " if  exists...");
        // Get database if exists

        database = client.getDatabase(databaseName);
        logger.info("Done.");
    }

    public void shutdown() {
        try {
            client.close();
        } catch (Exception err) {
            logger.error(
                    "Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }

        logger.info("Done with sample.");
    }

}
