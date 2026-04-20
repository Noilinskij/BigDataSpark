package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.bround;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.corr;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.year;

public class ClickHouseReportsJob {

    public static void main(String[] args) {
        String postgresJdbcUrl = getRequiredEnv("JDBC_URL");
        String postgresUser = getRequiredEnv("JDBC_USER");
        String postgresPassword = getRequiredEnv("JDBC_PASSWORD");

        String clickhouseJdbcUrl = getRequiredEnv("CLICKHOUSE_JDBC_URL");
        String clickhouseUser = getRequiredEnv("CLICKHOUSE_USER");
        String clickhousePassword = getRequiredEnv("CLICKHOUSE_PASSWORD");

        SparkSession spark = SparkSession.builder()
            .appName("postgres-snowflake-to-clickhouse-reports")
            .getOrCreate();

        try {
            runReports(
                spark,
                postgresJdbcUrl,
                postgresUser,
                postgresPassword,
                clickhouseJdbcUrl,
                clickhouseUser,
                clickhousePassword
            );
        } catch (Exception e) {
            System.err.println("ClickHouse reports job failed: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            spark.stop();
        }
    }

    private static void runReports(
        SparkSession spark,
        String postgresJdbcUrl,
        String postgresUser,
        String postgresPassword,
        String clickhouseJdbcUrl,
        String clickhouseUser,
        String clickhousePassword
    ) throws SQLException {
        Properties pgProps = buildJdbcProperties(postgresUser, postgresPassword, "org.postgresql.Driver");
        Properties chProps = buildJdbcProperties(clickhouseUser, clickhousePassword, "com.clickhouse.jdbc.ClickHouseDriver");

        recreateClickHouseTables(clickhouseJdbcUrl, clickhouseUser, clickhousePassword);

        Dataset<Row> factSales = readTable(spark, postgresJdbcUrl, "fact_sales", pgProps);
        Dataset<Row> product = readTable(spark, postgresJdbcUrl, "product", pgProps);
        Dataset<Row> productCategory = readTable(spark, postgresJdbcUrl, "product_category", pgProps);
        Dataset<Row> productBrand = readTable(spark, postgresJdbcUrl, "product_brand", pgProps);
        Dataset<Row> productColor = readTable(spark, postgresJdbcUrl, "product_color", pgProps);
        Dataset<Row> productSize = readTable(spark, postgresJdbcUrl, "product_size", pgProps);
        Dataset<Row> productMaterial = readTable(spark, postgresJdbcUrl, "product_material", pgProps);
        Dataset<Row> supplier = readTable(spark, postgresJdbcUrl, "supplier", pgProps);
        Dataset<Row> city = readTable(spark, postgresJdbcUrl, "city", pgProps);
        Dataset<Row> country = readTable(spark, postgresJdbcUrl, "country", pgProps);
        Dataset<Row> customer = readTable(spark, postgresJdbcUrl, "customer", pgProps);
        Dataset<Row> store = readTable(spark, postgresJdbcUrl, "store", pgProps);

        Dataset<Row> customerCountry = country.select(
            col("id").alias("customer_country_id"),
            col("name").alias("customer_country")
        );

        Dataset<Row> storeCountry = country.select(
            col("id").alias("store_country_id"),
            col("name").alias("store_country")
        );

        Dataset<Row> supplierCountry = country.select(
            col("id").alias("supplier_country_id"),
            col("name").alias("supplier_country")
        );

        Dataset<Row> storeCity = city.select(
            col("id").alias("store_city_id"),
            col("name").alias("store_city")
        );

        Dataset<Row> supplierCity = city.select(
            col("id").alias("supplier_city_id"),
            col("country_id").alias("supplier_city_country_id")
        );

        Dataset<Row> salesBase = factSales
            .join(
                product.select(
                    col("id").alias("product_id_ref"),
                    col("name").alias("product_name"),
                    col("category_id").alias("product_category_id"),
                    col("brand_id").alias("product_brand_id"),
                    col("color_id").alias("product_color_id"),
                    col("size_id").alias("product_size_id"),
                    col("material_id").alias("product_material_id"),
                    col("supplier_id").alias("supplier_id_ref"),
                    col("rating").alias("product_rating"),
                    col("reviews").alias("product_reviews")
                ),
                factSales.col("product_id").equalTo(col("product_id_ref")),
                "left"
            )
            .join(
                productCategory.select(
                    col("id").alias("product_category_id_ref"),
                    col("name").alias("product_category")
                ),
                col("product_category_id").equalTo(col("product_category_id_ref")),
                "left"
            )
            .join(
                productBrand.select(
                    col("id").alias("product_brand_id_ref"),
                    col("name").alias("product_brand")
                ),
                col("product_brand_id").equalTo(col("product_brand_id_ref")),
                "left"
            )
            .join(
                productColor.select(
                    col("id").alias("product_color_id_ref"),
                    col("name").alias("product_color")
                ),
                col("product_color_id").equalTo(col("product_color_id_ref")),
                "left"
            )
            .join(
                productSize.select(
                    col("id").alias("product_size_id_ref"),
                    col("name").alias("product_size")
                ),
                col("product_size_id").equalTo(col("product_size_id_ref")),
                "left"
            )
            .join(
                productMaterial.select(
                    col("id").alias("product_material_id_ref"),
                    col("name").alias("product_material")
                ),
                col("product_material_id").equalTo(col("product_material_id_ref")),
                "left"
            )
            .join(
                customer.select(
                    col("id").alias("customer_id_ref"),
                    col("first_name").alias("customer_first_name"),
                    col("last_name").alias("customer_last_name"),
                    col("country_id").alias("customer_country_id_ref")
                ),
                factSales.col("customer_id").equalTo(col("customer_id_ref")),
                "left"
            )
            .join(
                customerCountry,
                col("customer_country_id_ref").equalTo(customerCountry.col("customer_country_id")),
                "left"
            )
            .join(
                store.select(
                    col("id").alias("store_id_ref"),
                    col("name").alias("store_name"),
                    col("city_id").alias("store_city_id_ref"),
                    col("country_id").alias("store_country_id_ref")
                ),
                factSales.col("store_id").equalTo(col("store_id_ref")),
                "left"
            )
            .join(
                storeCity,
                col("store_city_id_ref").equalTo(storeCity.col("store_city_id")),
                "left"
            )
            .join(
                storeCountry,
                col("store_country_id_ref").equalTo(storeCountry.col("store_country_id")),
                "left"
            )
            .join(
                supplier.select(
                    col("id").alias("supplier_id_dim"),
                    col("name").alias("supplier_name"),
                    col("city_id").alias("supplier_city_id_ref")
                ),
                col("supplier_id_ref").equalTo(col("supplier_id_dim")),
                "left"
            )
            .join(
                supplierCity,
                col("supplier_city_id_ref").equalTo(supplierCity.col("supplier_city_id")),
                "left"
            )
            .join(
                supplierCountry,
                col("supplier_city_country_id").equalTo(supplierCountry.col("supplier_country_id")),
                "left"
            )
            .select(
                factSales.col("sale_date"),
                factSales.col("customer_id"),
                factSales.col("store_id"),
                factSales.col("quantity"),
                factSales.col("total_price"),
                factSales.col("unit_price"),
                col("product_id_ref").alias("product_id"),
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("product_rating"),
                col("product_reviews"),
                col("customer_first_name"),
                col("customer_last_name"),
                coalesce(col("customer_country"), lit("Unknown")).alias("customer_country"),
                col("store_name"),
                coalesce(col("store_city"), lit("Unknown")).alias("store_city"),
                coalesce(col("store_country"), lit("Unknown")).alias("store_country"),
                col("supplier_id_dim").alias("supplier_id"),
                col("supplier_name"),
                coalesce(col("supplier_country"), lit("Unknown")).alias("supplier_country")
            )
            .cache();

        WindowSpec productTopWindow = Window.orderBy(desc("total_quantity_sold"), desc("total_revenue"));
        Dataset<Row> productTop10 = salesBase
            .groupBy(
                col("product_id"),
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material")
            )
            .agg(
                sum(col("quantity")).cast("long").alias("total_quantity_sold"),
                bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("total_revenue")
            )
            .where(col("product_id").isNotNull())
            .withColumn("rn", row_number().over(productTopWindow))
            .where(col("rn").leq(10))
            .select(
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("total_quantity_sold")
            );
        writeTable(productTop10, clickhouseJdbcUrl, "product_top10_best_selling", chProps);

        Dataset<Row> productRevenueByCategory = salesBase
            .groupBy(col("product_category"))
            .agg(bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("category_revenue"))
            .where(col("product_category").isNotNull());
        writeTable(productRevenueByCategory, clickhouseJdbcUrl, "product_revenue_by_category", chProps);

        Dataset<Row> productRatingReviews = salesBase
            .groupBy(
                col("product_id"),
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material")
            )
            .agg(
                avg(col("product_rating").cast("double")).alias("avg_rating"),
                max(col("product_reviews").cast("long")).alias("reviews_count")
            )
            .where(col("product_id").isNotNull())
            .select(
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("avg_rating"),
                col("reviews_count")
            );
        writeTable(productRatingReviews, clickhouseJdbcUrl, "product_rating_reviews", chProps);

        WindowSpec customerTopWindow = Window.orderBy(desc("total_revenue"));
        Dataset<Row> customerTop10 = salesBase
            .withColumn("customer_full_name", concat_ws(" ", col("customer_first_name"), col("customer_last_name")))
            .groupBy(col("customer_id"), col("customer_full_name"))
            .agg(bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("total_revenue"))
            .where(col("customer_id").isNotNull())
            .withColumn("rn", row_number().over(customerTopWindow))
            .where(col("rn").leq(10))
            .select(
                col("customer_full_name"),
                col("total_revenue")
            );
        writeTable(customerTop10, clickhouseJdbcUrl, "customer_top10_by_spending", chProps);

        Dataset<Row> customerDistributionByCountry = salesBase
            .where(col("customer_id").isNotNull())
            .groupBy(col("customer_country"))
            .agg(count(col("customer_id")).cast("long").alias("customers_count"));
        writeTable(customerDistributionByCountry, clickhouseJdbcUrl, "customer_distribution_by_country", chProps);

        Dataset<Row> customerAverageCheck = salesBase
            .withColumn("customer_full_name", concat_ws(" ", col("customer_first_name"), col("customer_last_name")))
            .groupBy(col("customer_id"), col("customer_full_name"))
            .agg(
                bround(avg(col("total_price")), 2).cast("decimal(18,2)").alias("avg_check"),
                count(lit(1)).cast("long").alias("orders_count")
            )
            .where(col("customer_id").isNotNull())
            .select(
                col("customer_full_name"),
                col("avg_check")
            );
        writeTable(customerAverageCheck, clickhouseJdbcUrl, "customer_avg_check", chProps);

        Dataset<Row> timeMonthlyYearlyTrends = salesBase
            .withColumn("sales_year", year(col("sale_date")))
            .withColumn("sales_month", month(col("sale_date")))
            .groupBy(col("sales_year"), col("sales_month"))
            .agg(
                bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("revenue"),
                count(lit(1)).cast("long").alias("orders_count"),
                sum(col("quantity")).cast("long").alias("items_sold")
            )
            .select(
                col("sales_year"),
                col("sales_month"),
                col("revenue")
            );
        writeTable(timeMonthlyYearlyTrends, clickhouseJdbcUrl, "time_monthly_yearly_trends", chProps);

        Dataset<Row> periodComparisonBase = salesBase
            .withColumn("sales_year", year(col("sale_date")))
            .withColumn("sales_month", month(col("sale_date")))
            .groupBy(col("sales_year"), col("sales_month"))
            .agg(bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("period_revenue"))
            .withColumn("period_index", col("sales_year").multiply(lit(100)).plus(col("sales_month")));
        WindowSpec periodWindow = Window.orderBy(col("period_index"));
        Dataset<Row> timeRevenuePeriodComparison = periodComparisonBase
            .withColumn("prev_period_revenue", lag(col("period_revenue"), 1).over(periodWindow))
            .withColumn(
                "revenue_diff",
                bround(col("period_revenue").minus(coalesce(col("prev_period_revenue"), lit(0))), 2).cast("decimal(18,2)")
            )
            .withColumn(
                "revenue_growth_pct",
                when(col("prev_period_revenue").isNull().or(col("prev_period_revenue").equalTo(lit(0))), lit(null).cast("double"))
                    .otherwise(
                        bround(
                            col("period_revenue").minus(col("prev_period_revenue"))
                                .divide(col("prev_period_revenue"))
                                .multiply(lit(100)),
                            2
                        )
                    )
            )
            .select(
                col("sales_year"),
                col("sales_month"),
                col("period_revenue"),
                col("prev_period_revenue").cast("decimal(18,2)").alias("prev_period_revenue"),
                col("revenue_diff"),
                col("revenue_growth_pct")
            );
        writeTable(timeRevenuePeriodComparison, clickhouseJdbcUrl, "time_revenue_period_comparison", chProps);

        Dataset<Row> timeAverageOrderByMonth = salesBase
            .withColumn("sales_month", month(col("sale_date")))
            .groupBy(col("sales_month"))
            .agg(
                bround(avg(col("total_price")), 2).cast("decimal(18,2)").alias("avg_order_amount"),
                count(lit(1)).cast("long").alias("orders_count")
            )
            .select(
                col("sales_month"),
                col("avg_order_amount")
            );
        writeTable(timeAverageOrderByMonth, clickhouseJdbcUrl, "time_avg_order_by_month", chProps);

        WindowSpec storeTopWindow = Window.orderBy(desc("total_revenue"));
        Dataset<Row> storeTop5 = salesBase
            .groupBy(col("store_id"), col("store_name"))
            .agg(bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("total_revenue"))
            .where(col("store_id").isNotNull())
            .withColumn("rn", row_number().over(storeTopWindow))
            .where(col("rn").leq(5))
            .select(
                col("store_name"),
                col("total_revenue")
            );
        writeTable(storeTop5, clickhouseJdbcUrl, "store_top5_by_revenue", chProps);

        Dataset<Row> storeDistributionByCityCountry = salesBase
            .groupBy(col("store_country"), col("store_city"))
            .agg(
                count(lit(1)).cast("long").alias("sales_count"),
                bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("revenue")
            )
            .select(
                col("store_country"),
                col("store_city"),
                col("revenue")
            );
        writeTable(storeDistributionByCityCountry, clickhouseJdbcUrl, "store_distribution_city_country", chProps);

        Dataset<Row> storeAverageCheck = salesBase
            .groupBy(col("store_id"), col("store_name"))
            .agg(
                bround(avg(col("total_price")), 2).cast("decimal(18,2)").alias("avg_check"),
                count(lit(1)).cast("long").alias("orders_count")
            )
            .where(col("store_id").isNotNull())
            .select(
                col("store_name"),
                col("avg_check")
            );
        writeTable(storeAverageCheck, clickhouseJdbcUrl, "store_avg_check", chProps);

        WindowSpec supplierTopWindow = Window.orderBy(desc("total_revenue"));
        Dataset<Row> supplierTop5 = salesBase
            .groupBy(col("supplier_id"), col("supplier_name"))
            .agg(bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("total_revenue"))
            .where(col("supplier_id").isNotNull())
            .withColumn("rn", row_number().over(supplierTopWindow))
            .where(col("rn").leq(5))
            .select(
                col("supplier_name"),
                col("total_revenue")
            );
        writeTable(supplierTop5, clickhouseJdbcUrl, "supplier_top5_by_revenue", chProps);

        Dataset<Row> supplierAverageProductPrice = salesBase
            .groupBy(col("supplier_id"), col("supplier_name"))
            .agg(bround(avg(col("unit_price")), 2).cast("decimal(18,2)").alias("avg_product_price"))
            .where(col("supplier_id").isNotNull())
            .select(
                col("supplier_name"),
                col("avg_product_price")
            );
        writeTable(supplierAverageProductPrice, clickhouseJdbcUrl, "supplier_avg_product_price", chProps);

        Dataset<Row> supplierDistributionByCountry = salesBase
            .where(col("supplier_id").isNotNull())
            .groupBy(col("supplier_country"))
            .agg(
                count(lit(1)).cast("long").alias("sales_count"),
                bround(sum(col("total_price")), 2).cast("decimal(18,2)").alias("revenue")
            )
            .select(
                col("supplier_country"),
                col("revenue")
            );
        writeTable(supplierDistributionByCountry, clickhouseJdbcUrl, "supplier_distribution_by_country", chProps);

        Dataset<Row> qualityAgg = salesBase
            .groupBy(
                col("product_id"),
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material")
            )
            .agg(
                avg(col("product_rating").cast("double")).alias("avg_rating"),
                max(col("product_reviews").cast("long")).alias("reviews_count"),
                sum(col("quantity")).cast("long").alias("total_quantity_sold")
            )
            .where(col("product_id").isNotNull());

        WindowSpec highestRatingWindow = Window.orderBy(desc("avg_rating"), desc("reviews_count"), col("product_name"));
        WindowSpec lowestRatingWindow = Window.orderBy(col("avg_rating"), desc("reviews_count"), col("product_name"));

        Dataset<Row> highestProduct = qualityAgg
            .withColumn("rn", row_number().over(highestRatingWindow))
            .where(col("rn").equalTo(1))
            .select(
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("avg_rating")
            );

        Dataset<Row> lowestProduct = qualityAgg
            .withColumn("rn", row_number().over(lowestRatingWindow))
            .where(col("rn").equalTo(1))
            .select(
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("avg_rating")
            );

        Dataset<Row> qualityHighLow = highestProduct.unionByName(lowestProduct).distinct();
        writeTable(qualityHighLow, clickhouseJdbcUrl, "quality_products_high_low_rating", chProps);

        Dataset<Row> qualityCorrelation = qualityAgg
            .agg(
                corr(col("avg_rating"), col("total_quantity_sold").cast("double")).alias("rating_sales_correlation"),
                count(lit(1)).cast("long").alias("products_count")
            )
            .select(col("rating_sales_correlation"));
        writeTable(qualityCorrelation, clickhouseJdbcUrl, "quality_rating_sales_correlation", chProps);

        Dataset<Row> qualityMostReviewed = qualityAgg
            .orderBy(desc("reviews_count"), desc("avg_rating"), col("product_name"))
            .select(
                col("product_name"),
                col("product_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("reviews_count")
            );
        writeTable(qualityMostReviewed, clickhouseJdbcUrl, "quality_most_reviewed_products", chProps);

        salesBase.unpersist();
    }

    private static void recreateClickHouseTables(String jdbcUrl, String user, String password) throws SQLException {
        executeSqlBatch(jdbcUrl, user, password, Arrays.asList(
            "DROP TABLE IF EXISTS product_top10_best_selling",
            "DROP TABLE IF EXISTS product_revenue_by_category",
            "DROP TABLE IF EXISTS product_rating_reviews",
            "DROP TABLE IF EXISTS customer_top10_by_spending",
            "DROP TABLE IF EXISTS customer_distribution_by_country",
            "DROP TABLE IF EXISTS customer_avg_check",
            "DROP TABLE IF EXISTS time_monthly_yearly_trends",
            "DROP TABLE IF EXISTS time_revenue_period_comparison",
            "DROP TABLE IF EXISTS time_avg_order_by_month",
            "DROP TABLE IF EXISTS store_top5_by_revenue",
            "DROP TABLE IF EXISTS store_distribution_city_country",
            "DROP TABLE IF EXISTS store_avg_check",
            "DROP TABLE IF EXISTS supplier_top5_by_revenue",
            "DROP TABLE IF EXISTS supplier_avg_product_price",
            "DROP TABLE IF EXISTS supplier_distribution_by_country",
            "DROP TABLE IF EXISTS quality_products_high_low_rating",
            "DROP TABLE IF EXISTS quality_rating_sales_correlation",
            "DROP TABLE IF EXISTS quality_most_reviewed_products",

            "CREATE TABLE product_top10_best_selling ("
                + "product_name String,"
                + "product_category Nullable(String),"
                + "product_brand Nullable(String),"
                + "product_color Nullable(String),"
                + "product_size Nullable(String),"
                + "product_material Nullable(String),"
                + "total_quantity_sold Int64"
                + ") ENGINE = MergeTree ORDER BY (product_name)",

            "CREATE TABLE product_revenue_by_category ("
                + "product_category String,"
                + "category_revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (product_category)",

            "CREATE TABLE product_rating_reviews ("
                + "product_name String,"
                + "product_category Nullable(String),"
                + "product_brand Nullable(String),"
                + "product_color Nullable(String),"
                + "product_size Nullable(String),"
                + "product_material Nullable(String),"
                + "avg_rating Float64,"
                + "reviews_count Int64"
                + ") ENGINE = MergeTree ORDER BY (product_name)",

            "CREATE TABLE customer_top10_by_spending ("
                + "customer_full_name String,"
                + "total_revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (customer_full_name)",

            "CREATE TABLE customer_distribution_by_country ("
                + "customer_country String,"
                + "customers_count Int64"
                + ") ENGINE = MergeTree ORDER BY (customer_country)",

            "CREATE TABLE customer_avg_check ("
                + "customer_full_name String,"
                + "avg_check Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (customer_full_name)",

            "CREATE TABLE time_monthly_yearly_trends ("
                + "sales_year Int32,"
                + "sales_month Int32,"
                + "revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (sales_year, sales_month)",

            "CREATE TABLE time_revenue_period_comparison ("
                + "sales_year Int32,"
                + "sales_month Int32,"
                + "period_revenue Decimal(18,2),"
                + "prev_period_revenue Nullable(Decimal(18,2)),"
                + "revenue_diff Decimal(18,2),"
                + "revenue_growth_pct Nullable(Float64)"
                + ") ENGINE = MergeTree ORDER BY (sales_year, sales_month)",

            "CREATE TABLE time_avg_order_by_month ("
                + "sales_month Int32,"
                + "avg_order_amount Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (sales_month)",

            "CREATE TABLE store_top5_by_revenue ("
                + "store_name String,"
                + "total_revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (store_name)",

            "CREATE TABLE store_distribution_city_country ("
                + "store_country String,"
                + "store_city String,"
                + "revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (store_country, store_city)",

            "CREATE TABLE store_avg_check ("
                + "store_name String,"
                + "avg_check Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (store_name)",

            "CREATE TABLE supplier_top5_by_revenue ("
                + "supplier_name String,"
                + "total_revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (supplier_name)",

            "CREATE TABLE supplier_avg_product_price ("
                + "supplier_name String,"
                + "avg_product_price Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (supplier_name)",

            "CREATE TABLE supplier_distribution_by_country ("
                + "supplier_country String,"
                + "revenue Decimal(18,2)"
                + ") ENGINE = MergeTree ORDER BY (supplier_country)",

            "CREATE TABLE quality_products_high_low_rating ("
                + "product_name String,"
                + "product_category Nullable(String),"
                + "product_brand Nullable(String),"
                + "product_color Nullable(String),"
                + "product_size Nullable(String),"
                + "product_material Nullable(String),"
                + "avg_rating Float64"
                + ") ENGINE = MergeTree ORDER BY (product_name)",

            "CREATE TABLE quality_rating_sales_correlation ("
                + "rating_sales_correlation Nullable(Float64)"
                + ") ENGINE = MergeTree ORDER BY tuple()",

            "CREATE TABLE quality_most_reviewed_products ("
                + "product_name String,"
                + "product_category Nullable(String),"
                + "product_brand Nullable(String),"
                + "product_color Nullable(String),"
                + "product_size Nullable(String),"
                + "product_material Nullable(String),"
                + "reviews_count Int64"
                + ") ENGINE = MergeTree ORDER BY (product_name)"
        ));
    }

    private static Dataset<Row> readTable(SparkSession spark, String jdbcUrl, String tableName, Properties jdbcProps) {
        return spark.read().jdbc(jdbcUrl, tableName, jdbcProps);
    }

    private static void writeTable(Dataset<Row> df, String jdbcUrl, String tableName, Properties jdbcProps) {
        df.write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, tableName, jdbcProps);
    }

    private static Properties buildJdbcProperties(String user, String password, String driver) {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("driver", driver);
        return props;
    }

    private static void executeSqlBatch(String jdbcUrl, String user, String password, List<String> statements) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement stmt = conn.createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        }
    }

    private static String getRequiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + key);
        }
        return value;
    }
}
