package com.badoo;

import com.badoo.server.GenericExceptionMapper;
import com.badoo.server.HiveProxyResource;
import lombok.Data;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.status.api.v1.JacksonMessageWriter;
import org.apache.spark.ui.SparkUI;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.spark_project.jetty.servlet.ServletContextHandler;
import org.spark_project.jetty.servlet.ServletHolder;


/**
 * Created by krash on 02.03.18.
 * <p>
 */
public class HiveProxy {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String... argv) throws Exception {

//        if (true) {
//
//            try {
//                Class.forName(driverName);
//            } catch (ClassNotFoundException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//                System.exit(1);
//            }
//            Connection con = DriverManager.getConnection("jdbc:hive2://bi1.mlan:10000/default", "", "");
//            con.createStatement().execute("ADD JAR /local/hive/lib/json.jar");
//
//            con.createStatement().execute("INSERT OVERWRITE TABLE tmp_staging_f_app_crashed_1\n" +
//                    "SELECT a.crash_ts\n" +
//                    "    , a.brand\n" +
//                    "    , a.platform\n" +
//                    "    , a.app_version\n" +
//                    "    , COALESCE(b.app_crashed_count, 0) AS app_crashed_count\n" +
//                    "    , COALESCE(a.app_starts_count, 0) AS app_starts_count\n" +
//                    "    , COALESCE(a.unique_users_count, 0) AS unique_users_count\n" +
//                    "    , COALESCE(b.app_crashed_handled_count, 0) AS app_crashed_handled_count\n" +
//                    "    , COALESCE(b.app_crashed_anr_count, 0) AS app_crashed_anr_count\n" +
//                    "    , COALESCE(b.app_crashed_out_of_memory_count, 0) AS app_crashed_out_of_memory_count\n" +
//                    "FROM (\n" +
//                    "    SELECT date_format(ts, 'yyyy-MM-dd HH:00:00') AS crash_ts\n" +
//                    "        , application.brand AS brand\n" +
//                    "        , application.platform AS platform\n" +
//                    "        , application.app_version AS app_version\n" +
//                    "        , count(1) AS app_starts_count\n" +
//                    "        , count(distinct `user`.user_id) AS unique_users_count\n" +
//                    "    FROM uds.hotpanel_appstartevent\n" +
//                    "    WHERE dt BETWEEN '2018-02-26' AND '2018-02-27'\n" +
//                    "        AND ts IS NOT NULL\n" +
//                    "        AND application.app_version REGEXP '^([0-9]+)\\.([0-9]+)\\.([0-9]+)(\\-[A-Z]+)?$'\n" +
//                    "    GROUP BY date_format(ts, 'yyyy-MM-dd HH:00:00')\n" +
//                    "        , application.brand\n" +
//                    "        , application.platform\n" +
//                    "        , application.app_version\n" +
//                    ") a\n" +
//                    "LEFT JOIN (\n" +
//                    "    SELECT date_format(ts, 'yyyy-MM-dd HH:00:00') AS crash_ts\n" +
//                    "        , application.brand AS brand\n" +
//                    "        , application.platform AS platform\n" +
//                    "        , application.app_version AS app_version\n" +
//                    "        , count(1) AS app_crashed_count\n" +
//                    "        , count(CASE WHEN hp_is_handled = 1 THEN 1 END) AS app_crashed_handled_count\n" +
//                    "        , count(CASE WHEN hp_is_anr = 1 THEN 1 END) AS app_crashed_anr_count\n" +
//                    "        , count(CASE WHEN hp_is_out_of_memory = 1 THEN 1 END) AS app_crashed_out_of_memory_count\n" +
//                    "    FROM uds.hotpanel_appcrashedevent\n" +
//                    "    WHERE dt BETWEEN '2018-02-26' AND '2018-02-27'\n" +
//                    "        AND ts IS NOT NULL\n" +
//                    "    GROUP BY date_format(ts, 'yyyy-MM-dd HH:00:00')\n" +
//                    "        , application.brand\n" +
//                    "        , application.platform\n" +
//                    "        , application.app_version\n" +
//                    ") b ON (a.crash_ts=b.crash_ts AND a.brand=b.brand AND a.platform=b.platform AND a.app_version=b.app_version)");
//
//            if (true) return;
//
//            con.createStatement().execute("CREATE EXTERNAL TABLE tmp_staging_f_app_crashed_1\n" +
//                    "(\n" +
//                    "    `crash_ts` string,\n" +
//                    "    `brand` int,\n" +
//                    "    `platform` int,\n" +
//                    "    `app_version` string,\n" +
//                    "    `app_crashed_count` bigint,\n" +
//                    "    `app_starts_count` bigint,\n" +
//                    "    `unique_users_count` bigint,\n" +
//                    "    `app_crashed_handled_count` bigint,\n" +
//                    "    `app_crashed_anr_count` bigint,\n" +
//                    "    `app_crashed_out_of_memory_count` bigint\n" +
//                    ")\n" +
//                    "ROW FORMAT DELIMITED\n" +
//                    "FIELDS TERMINATED BY '\\t'\n" +
//                    "LINES TERMINATED BY '\\n'\n" +
//                    "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n" +
//                    "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
//                    "LOCATION 'hdfs://bihadoop/tmp/tmp_staging_f_app_crashed_1'");
//
//            return;
//        }

        SparkConf conf = new SparkConf(true);
        conf.setMaster("local[*]").setAppName("Hive proxy");
        conf.set("spark.ui.port", "5000");
        JavaSparkContext ctx = new JavaSparkContext(conf);

//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[10]")
//                .appName("Hive proxy")
//                .enableHiveSupport()
//                .getOrCreate();
//        spark.sql("SHOW DATABASES").show(10);

        // init hive api
        final SparkUI sparkUI = ctx.sc().ui().get();
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/hive-proxy");

        ResourceConfig config = new ResourceConfig();
        config.register(new HiveProxyResource(ctx));
        config.register(GenericExceptionMapper.class);
        config.register(JacksonMessageWriter.class);
        ServletHolder holder = new ServletHolder(new ServletContainer(config));
        handler.addServlet(holder, "/*");
        sparkUI.attachHandler(handler);

        while (!ctx.sc().stopped().get()) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
        }
    }


    @Data
    private static class Config {
        private final int port;
    }
}
