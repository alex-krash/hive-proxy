package com.badoo.server;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by krash on 05.03.18.
 */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Data
@Slf4j
public class HiveProxyResource {

    private final JavaSparkContext ctx;

    @POST
    @Path("query")
    public void runQuery(@Context UriInfo uri, @QueryParam("user") String user, MultivaluedMap<String, String> formParams) throws Exception {
        List<String> queries = new ArrayList<>();
        formParams.entrySet().forEach(new Consumer<Map.Entry<String, List<String>>>() {
            @Override
            public void accept(Map.Entry<String, List<String>> stringListEntry) {
                if (stringListEntry.getKey().startsWith("query[")) {
                    queries.addAll(stringListEntry.getValue());
                }
            }
        });
        execute("hadoop", queries);
    }

    private void execute(String user, List<String> queries) throws Exception {
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("Query list is empty");
        }
        final UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(user);

        for (Token<? extends TokenIdentifier> t : UserGroupInformation.getCurrentUser().getTokens()) {
            remoteUser.addToken(t);
        }

        System.out.println(UserGroupInformation.getLoginUser().getUserName());

        Object r = remoteUser.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {

                UserGroupInformation.setLoginUser(remoteUser);

                try {
                    SparkSession spark = SparkSession
                            .builder()
                            .master("local[10]")
                            .appName("Hive proxy")
                            .enableHiveSupport()
                            .getOrCreate();
                    for (String query : queries) {
                        long start = System.currentTimeMillis();
                        System.out.println("Executing " + query);
                        try {
//                            spark.sparkContext().addJar("/local/hive/lib/json.jar");
                            spark.sql(query).show(10, false);
                        } catch (Exception err) {
                            log.error("Error in query " + query, err);
                            throw err;
                        }

                        System.out.println("Complete in " + (System.currentTimeMillis() - start));
                    }
                } finally {
                    SparkSession.setDefaultSession(null);
                    SparkSession.setActiveSession(null);
                }
                return null;
            }
        });
    }

    @GET
    @Path("time")
    public long getTime() throws Exception {
        return System.currentTimeMillis();
    }

}
