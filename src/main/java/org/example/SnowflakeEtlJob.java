package org.example;

import org.apache.spark.sql.Column;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

public class SnowflakeEtlJob {

    public static void main(String[] args) {
        String csvPath = getRequiredEnv("CSV_PATH");
        String jdbcUrl = getRequiredEnv("JDBC_URL");
        String jdbcUser = getRequiredEnv("JDBC_USER");
        String jdbcPassword = getRequiredEnv("JDBC_PASSWORD");

        SparkSession spark = SparkSession.builder()
                .appName("mock-data-to-postgres-snowflake")
                .getOrCreate();

        try {
            runEtl(spark, csvPath, jdbcUrl, jdbcUser, jdbcPassword);
        } catch (Exception e) {
            System.err.println("ETL failed: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            spark.stop();
        }
    }

    private static void runEtl( SparkSession spark, String csvPath, String jdbcUrl, String jdbcUser, String jdbcPassword) throws SQLException {
        Properties jdbcProps = buildJdbcProperties(jdbcUser, jdbcPassword);

        Dataset<Row> sourceDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("quote", "\"")
            .option("escape", "\"")
                .option("mode", "FAILFAST")
                .csv(csvPath);

        Dataset<Row> typedDf = sourceDf.cache();

        recreateSourceTable(jdbcUrl, jdbcUser, jdbcPassword);
        typedDf.write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "public.mock_data_all", jdbcProps);

        recreateSnowflakeSchema(jdbcUrl, jdbcUser, jdbcPassword);

        Dataset<Row> normalized = typedDf
            .withColumn("store_state_norm", nullIfEmpty(col("store_state")))
            .withColumn("customer_postal_code_norm", nullIfEmpty(col("customer_postal_code")))
            .withColumn("seller_postal_code_norm", nullIfEmpty(col("seller_postal_code")));

        Dataset<Row> country = normalized
            .select(col("customer_country").alias("name"))
            .union(normalized.select(col("seller_country").alias("name")))
            .union(normalized.select(col("store_country").alias("name")))
            .union(normalized.select(col("supplier_country").alias("name")))
            .where(col("name").isNotNull())
            .distinct();
        writeTable(country, jdbcUrl, "country", jdbcProps);

        Dataset<Row> countryRef = readTable(spark, jdbcUrl, "country", jdbcProps)
            .select(col("id").cast("long").alias("country_id"), col("name").alias("country_name"));

        Dataset<Row> state = normalized
            .where(col("store_state_norm").isNotNull())
            .join(countryRef, normalized.col("store_country").equalTo(countryRef.col("country_name")), "inner")
            .select(
                col("store_state_norm").alias("name"),
                col("country_id").alias("country_id")
            )
            .distinct();
        writeTable(state, jdbcUrl, "state", jdbcProps);

        Dataset<Row> stateRef = readTable(spark, jdbcUrl, "state", jdbcProps)
            .select(
                col("id").cast("long").alias("state_id"),
                col("name").alias("state_name"),
                col("country_id").cast("long").alias("state_country_id")
            );

        Dataset<Row> cityFromStore = normalized
            .where(col("store_city").isNotNull())
            .join(countryRef, normalized.col("store_country").equalTo(countryRef.col("country_name")), "inner")
            .join(
                stateRef,
                normalized.col("store_state_norm").equalTo(stateRef.col("state_name"))
                    .and(stateRef.col("state_country_id").equalTo(countryRef.col("country_id"))),
                "left"
            )
            .select(
                normalized.col("store_city").alias("name"),
                stateRef.col("state_id").alias("state_id"),
                countryRef.col("country_id").alias("country_id")
            );

        Dataset<Row> cityFromSupplier = normalized
            .where(col("supplier_city").isNotNull())
            .join(countryRef, normalized.col("supplier_country").equalTo(countryRef.col("country_name")), "inner")
            .select(
                col("supplier_city").alias("name"),
                lit(null).cast("long").alias("state_id"),
                col("country_id").alias("country_id")
            );

        Dataset<Row> city = cityFromStore.unionByName(cityFromSupplier).distinct();
        writeTable(city, jdbcUrl, "city", jdbcProps);

        Dataset<Row> cityRef = readTable(spark, jdbcUrl, "city", jdbcProps)
            .select(
                col("id").cast("long").alias("city_id"),
                col("name").alias("city_name"),
                col("state_id").cast("long").alias("city_state_id"),
                col("country_id").cast("long").alias("city_country_id")
            );

        writeNamedDimension(normalized, "product_category", "product_category", jdbcUrl, jdbcProps);
        writeNamedDimension(normalized, "product_brand", "product_brand", jdbcUrl, jdbcProps);
        writeNamedDimension(normalized, "product_material", "product_material", jdbcUrl, jdbcProps);
        writeNamedDimension(normalized, "product_color", "product_color", jdbcUrl, jdbcProps);
        writeNamedDimension(normalized, "product_size", "product_size", jdbcUrl, jdbcProps);
        writeNamedDimension(normalized, "pet_category", "pet_category", jdbcUrl, jdbcProps);

        Dataset<Row> petCategoryRef = readTable(spark, jdbcUrl, "pet_category", jdbcProps)
            .select(col("id").cast("long").alias("pet_category_id"), col("name").alias("pet_category_name"));
        Dataset<Row> productCategoryRef = readTable(spark, jdbcUrl, "product_category", jdbcProps)
            .select(col("id").cast("long").alias("category_id"), col("name").alias("category_name"));
        Dataset<Row> productBrandRef = readTable(spark, jdbcUrl, "product_brand", jdbcProps)
            .select(col("id").cast("long").alias("brand_id"), col("name").alias("brand_name"));
        Dataset<Row> productMaterialRef = readTable(spark, jdbcUrl, "product_material", jdbcProps)
            .select(col("id").cast("long").alias("material_id"), col("name").alias("material_name"));
        Dataset<Row> productColorRef = readTable(spark, jdbcUrl, "product_color", jdbcProps)
            .select(col("id").cast("long").alias("color_id"), col("name").alias("color_name"));
        Dataset<Row> productSizeRef = readTable(spark, jdbcUrl, "product_size", jdbcProps)
            .select(col("id").cast("long").alias("size_id"), col("name").alias("size_name"));

        Dataset<Row> supplier = normalized
            .where(col("supplier_name").isNotNull())
            .select(
                col("supplier_name").alias("name"),
                col("supplier_contact").alias("contact"),
                col("supplier_email").alias("email"),
                col("supplier_phone").alias("phone"),
                col("supplier_address").alias("address"),
                col("supplier_city"),
                col("supplier_country")
            )
            .distinct()
            .join(countryRef, col("supplier_country").equalTo(countryRef.col("country_name")), "left")
            .join(
                cityRef,
                col("supplier_city").equalTo(cityRef.col("city_name"))
                    .and(cityRef.col("city_country_id").equalTo(countryRef.col("country_id")))
                    .and(cityRef.col("city_state_id").isNull()),
                "left"
            )
            .select(
                col("name"),
                col("contact"),
                col("email"),
                col("phone"),
                col("address"),
                col("city_id")
            )
            .distinct();
        writeTable(supplier, jdbcUrl, "supplier", jdbcProps);

        Dataset<Row> supplierRef = readTable(spark, jdbcUrl, "supplier", jdbcProps)
            .select(
                col("id").cast("long").alias("supplier_id"),
                col("name").alias("supplier_name"),
                col("email").alias("supplier_email"),
                col("phone").alias("supplier_phone")
            );

        WindowSpec byProduct = Window.partitionBy(col("sale_product_id")).orderBy(col("id").cast("int"));
        Dataset<Row> productBase = normalized
            .where(col("sale_product_id").isNotNull())
            .withColumn("rn", row_number().over(byProduct))
            .where(col("rn").equalTo(1));

        Dataset<Row> product = productBase
            .join(petCategoryRef, productBase.col("pet_category").equalTo(petCategoryRef.col("pet_category_name")), "left")
            .join(productCategoryRef, productBase.col("product_category").equalTo(productCategoryRef.col("category_name")), "left")
            .join(productBrandRef, productBase.col("product_brand").equalTo(productBrandRef.col("brand_name")), "left")
            .join(productMaterialRef, productBase.col("product_material").equalTo(productMaterialRef.col("material_name")), "left")
            .join(productColorRef, productBase.col("product_color").equalTo(productColorRef.col("color_name")), "left")
            .join(productSizeRef, productBase.col("product_size").equalTo(productSizeRef.col("size_name")), "left")
            .join(
                supplierRef,
                productBase.col("supplier_name").equalTo(supplierRef.col("supplier_name"))
                    .and(productBase.col("supplier_email").eqNullSafe(supplierRef.col("supplier_email")))
                    .and(productBase.col("supplier_phone").eqNullSafe(supplierRef.col("supplier_phone"))),
                "left"
            )
            .select(
                productBase.col("sale_product_id").cast("long").alias("id"),
                productBase.col("product_name").alias("name"),
                col("pet_category_id"),
                col("category_id"),
                col("brand_id"),
                col("material_id"),
                col("color_id"),
                col("size_id"),
                col("supplier_id"),
                productBase.col("product_quantity").cast("int").alias("quantity"),
                productBase.col("product_weight").cast("decimal(12,3)").alias("weight"),
                productBase.col("product_rating").cast("decimal(4,2)").alias("rating"),
                productBase.col("product_reviews").cast("int").alias("reviews"),
                to_date(productBase.col("product_release_date"), "M/d/yyyy").alias("release_date"),
                to_date(productBase.col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
                productBase.col("product_description").alias("description")
            );
        writeTable(product, jdbcUrl, "product", jdbcProps);

        Dataset<Row> petBreed = normalized
            .where(col("customer_pet_type").isNotNull().and(col("customer_pet_breed").isNotNull()))
            .select(
                col("customer_pet_type").alias("type_name"),
                col("customer_pet_breed").alias("breed_name")
            )
            .distinct();
        writeTable(petBreed, jdbcUrl, "pet_breed", jdbcProps);

        Dataset<Row> petBreedRef = readTable(spark, jdbcUrl, "pet_breed", jdbcProps)
            .select(
                col("id").cast("long").alias("pet_breed_id"),
                col("type_name").alias("pet_type_name"),
                col("breed_name").alias("pet_breed_name")
            );

        Dataset<Row> pet = normalized
            .where(col("customer_pet_name").isNotNull())
            .join(
                petBreedRef,
                normalized.col("customer_pet_type").equalTo(petBreedRef.col("pet_type_name"))
                    .and(normalized.col("customer_pet_breed").equalTo(petBreedRef.col("pet_breed_name"))),
                "inner"
            )
            .select(
                col("customer_pet_name").alias("name"),
                col("pet_breed_id")
            )
            .distinct();
        writeTable(pet, jdbcUrl, "pet", jdbcProps);

        Dataset<Row> petRef = readTable(spark, jdbcUrl, "pet", jdbcProps)
            .select(
                col("id").cast("long").alias("pet_id"),
                col("name").alias("pet_name"),
                col("pet_breed_id").cast("long").alias("pet_pet_breed_id")
            );

        WindowSpec byCustomer = Window.partitionBy(col("sale_customer_id")).orderBy(col("id").cast("int"));
        Dataset<Row> customerBase = normalized
            .where(col("sale_customer_id").isNotNull())
            .withColumn("rn", row_number().over(byCustomer))
            .where(col("rn").equalTo(1));

        Dataset<Row> customer = customerBase
            .join(countryRef, customerBase.col("customer_country").equalTo(countryRef.col("country_name")), "left")
            .join(
                petBreedRef,
                customerBase.col("customer_pet_type").equalTo(petBreedRef.col("pet_type_name"))
                    .and(customerBase.col("customer_pet_breed").equalTo(petBreedRef.col("pet_breed_name"))),
                "left"
            )
            .join(
                petRef,
                customerBase.col("customer_pet_name").equalTo(petRef.col("pet_name"))
                    .and(petRef.col("pet_pet_breed_id").equalTo(petBreedRef.col("pet_breed_id"))),
                "left"
            )
            .select(
                customerBase.col("sale_customer_id").cast("long").alias("id"),
                customerBase.col("customer_first_name").alias("first_name"),
                customerBase.col("customer_last_name").alias("last_name"),
                customerBase.col("customer_age").cast("int").alias("age"),
                customerBase.col("customer_email").alias("email"),
                customerBase.col("customer_postal_code_norm").alias("postal_code"),
                countryRef.col("country_id").alias("country_id"),
                petRef.col("pet_id").alias("pet_id")
            );
        writeTable(customer, jdbcUrl, "customer", jdbcProps);

        WindowSpec bySeller = Window.partitionBy(col("sale_seller_id")).orderBy(col("id").cast("int"));
        Dataset<Row> sellerBase = normalized
            .where(col("sale_seller_id").isNotNull())
            .withColumn("rn", row_number().over(bySeller))
            .where(col("rn").equalTo(1));

        Dataset<Row> seller = sellerBase
            .join(countryRef, sellerBase.col("seller_country").equalTo(countryRef.col("country_name")), "left")
            .select(
                sellerBase.col("sale_seller_id").cast("long").alias("id"),
                sellerBase.col("seller_first_name").alias("first_name"),
                sellerBase.col("seller_last_name").alias("last_name"),
                sellerBase.col("seller_email").alias("email"),
                sellerBase.col("seller_postal_code_norm").alias("postal_code"),
                countryRef.col("country_id").alias("country_id")
            );
        writeTable(seller, jdbcUrl, "seller", jdbcProps);

        Dataset<Row> store = normalized
            .where(col("store_name").isNotNull())
            .join(countryRef, normalized.col("store_country").equalTo(countryRef.col("country_name")), "left")
            .join(
                stateRef,
                normalized.col("store_state_norm").equalTo(stateRef.col("state_name"))
                    .and(stateRef.col("state_country_id").equalTo(countryRef.col("country_id"))),
                "left"
            )
            .join(
                cityRef,
                normalized.col("store_city").equalTo(cityRef.col("city_name"))
                    .and(cityRef.col("city_country_id").equalTo(countryRef.col("country_id")))
                    .and(cityRef.col("city_state_id").eqNullSafe(stateRef.col("state_id"))),
                "left"
            )
            .select(
                normalized.col("store_name").alias("name"),
                normalized.col("store_location").alias("location"),
                normalized.col("store_phone").alias("phone"),
                normalized.col("store_email").alias("email"),
                cityRef.col("city_id").alias("city_id"),
                stateRef.col("state_id").alias("state_id"),
                countryRef.col("country_id").alias("country_id")
            )
            .distinct();
        writeTable(store, jdbcUrl, "store", jdbcProps);

        Dataset<Row> storeRef = readTable(spark, jdbcUrl, "store", jdbcProps)
            .select(
                col("id").cast("long").alias("store_id"),
                col("name").alias("store_name_ref"),
                col("location").alias("store_location_ref"),
                col("city_id").cast("long").alias("store_city_id_ref")
            );

        Dataset<Row> factSales = normalized
            .withColumn("sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))
            .join(countryRef, normalized.col("store_country").equalTo(countryRef.col("country_name")), "inner")
            .join(
                stateRef,
                normalized.col("store_state_norm").equalTo(stateRef.col("state_name"))
                    .and(stateRef.col("state_country_id").equalTo(countryRef.col("country_id"))),
                "left"
            )
            .join(
                cityRef,
                normalized.col("store_city").equalTo(cityRef.col("city_name"))
                    .and(cityRef.col("city_country_id").equalTo(countryRef.col("country_id")))
                    .and(cityRef.col("city_state_id").eqNullSafe(stateRef.col("state_id"))),
                "inner"
            )
            .join(
                storeRef,
                normalized.col("store_name").equalTo(storeRef.col("store_name_ref"))
                    .and(normalized.col("store_location").eqNullSafe(storeRef.col("store_location_ref")))
                    .and(storeRef.col("store_city_id_ref").equalTo(cityRef.col("city_id"))),
                "inner"
            )
            .where(col("sale_date_parsed").isNotNull())
            .where(col("sale_customer_id").isNotNull())
            .where(col("sale_seller_id").isNotNull())
            .where(col("sale_product_id").isNotNull())
            .where(col("sale_quantity").isNotNull())
            .where(col("sale_total_price").isNotNull())
            .select(
                col("sale_date_parsed").alias("sale_date"),
                col("sale_customer_id").cast("long").alias("customer_id"),
                col("sale_seller_id").cast("long").alias("seller_id"),
                col("sale_product_id").cast("long").alias("product_id"),
                col("store_id").alias("store_id"),
                col("sale_quantity").cast("int").alias("quantity"),
                col("sale_total_price").cast("decimal(14,2)").alias("total_price"),
                col("product_price").cast("decimal(14,2)").alias("unit_price")
            );
        writeTable(factSales, jdbcUrl, "fact_sales", jdbcProps);

        typedDf.unpersist();
        }

        private static void recreateSourceTable(String jdbcUrl, String user, String password) throws SQLException {
            executeSqlBatch(jdbcUrl, user, password, Arrays.asList(
                "DROP TABLE IF EXISTS public.mock_data_all",
                "CREATE TABLE public.mock_data_all ("
                + "id TEXT,"
                + "customer_first_name TEXT,"
                + "customer_last_name TEXT,"
                + "customer_age TEXT,"
                + "customer_email TEXT,"
                + "customer_country TEXT,"
                + "customer_postal_code TEXT,"
                + "customer_pet_type TEXT,"
                + "customer_pet_name TEXT,"
                + "customer_pet_breed TEXT,"
                + "seller_first_name TEXT,"
                + "seller_last_name TEXT,"
                + "seller_email TEXT,"
                + "seller_country TEXT,"
                + "seller_postal_code TEXT,"
                + "product_name TEXT,"
                + "product_category TEXT,"
                + "product_price TEXT,"
                + "product_quantity TEXT,"
                + "sale_date TEXT,"
                + "sale_customer_id TEXT,"
                + "sale_seller_id TEXT,"
                + "sale_product_id TEXT,"
                + "sale_quantity TEXT,"
                + "sale_total_price TEXT,"
                + "store_name TEXT,"
                + "store_location TEXT,"
                + "store_city TEXT,"
                + "store_state TEXT,"
                + "store_country TEXT,"
                + "store_phone TEXT,"
                + "store_email TEXT,"
                + "pet_category TEXT,"
                + "product_weight TEXT,"
                + "product_color TEXT,"
                + "product_size TEXT,"
                + "product_brand TEXT,"
                + "product_material TEXT,"
                + "product_description TEXT,"
                + "product_rating TEXT,"
                + "product_reviews TEXT,"
                + "product_release_date TEXT,"
                + "product_expiry_date TEXT,"
                + "supplier_name TEXT,"
                + "supplier_contact TEXT,"
                + "supplier_email TEXT,"
                + "supplier_phone TEXT,"
                + "supplier_address TEXT,"
                + "supplier_city TEXT,"
                + "supplier_country TEXT"
                + ")"
        ));
        }

        private static void recreateSnowflakeSchema(String jdbcUrl, String user, String password) throws SQLException {
        executeSqlBatch(jdbcUrl, user, password, Arrays.asList(
            "DROP TABLE IF EXISTS fact_sales CASCADE",
            "DROP TABLE IF EXISTS customer CASCADE",
            "DROP TABLE IF EXISTS pet CASCADE",
            "DROP TABLE IF EXISTS seller CASCADE",
            "DROP TABLE IF EXISTS store CASCADE",
            "DROP TABLE IF EXISTS supplier CASCADE",
            "DROP TABLE IF EXISTS product CASCADE",
            "DROP TABLE IF EXISTS city CASCADE",
            "DROP TABLE IF EXISTS state CASCADE",
            "DROP TABLE IF EXISTS country CASCADE",
            "DROP TABLE IF EXISTS pet_breed CASCADE",
            "DROP TABLE IF EXISTS product_category CASCADE",
            "DROP TABLE IF EXISTS product_brand CASCADE",
            "DROP TABLE IF EXISTS product_material CASCADE",
            "DROP TABLE IF EXISTS product_color CASCADE",
            "DROP TABLE IF EXISTS product_size CASCADE",
            "DROP TABLE IF EXISTS pet_category CASCADE",
            "CREATE TABLE country (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_country_name UNIQUE (name))",
            "CREATE TABLE state (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, country_id BIGINT NOT NULL REFERENCES country(id), CONSTRAINT uq_state_name_country UNIQUE (name, country_id))",
            "CREATE TABLE city (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, state_id BIGINT REFERENCES state(id), country_id BIGINT NOT NULL REFERENCES country(id), CONSTRAINT uq_city_name_state_country UNIQUE (name, state_id, country_id))",
            "CREATE TABLE product_category (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_product_category_name UNIQUE (name))",
            "CREATE TABLE product_brand (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_product_brand_name UNIQUE (name))",
            "CREATE TABLE product_material (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_product_material_name UNIQUE (name))",
            "CREATE TABLE product_color (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_product_color_name UNIQUE (name))",
            "CREATE TABLE product_size (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_product_size_name UNIQUE (name))",
            "CREATE TABLE pet_category (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_pet_category_name UNIQUE (name))",
            "CREATE TABLE supplier (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, contact TEXT, email TEXT, phone TEXT, address TEXT, city_id BIGINT REFERENCES city(id), CONSTRAINT uq_supplier_name_email_phone UNIQUE (name, email, phone))",
            "CREATE TABLE product (id BIGINT PRIMARY KEY, name TEXT NOT NULL, pet_category_id BIGINT REFERENCES pet_category(id), category_id BIGINT REFERENCES product_category(id), brand_id BIGINT REFERENCES product_brand(id), material_id BIGINT REFERENCES product_material(id), color_id BIGINT REFERENCES product_color(id), size_id BIGINT REFERENCES product_size(id), supplier_id BIGINT REFERENCES supplier(id), quantity INTEGER, weight NUMERIC(12,3), rating NUMERIC(4,2), reviews INTEGER, release_date DATE, expiry_date DATE, description TEXT)",
            "CREATE TABLE pet_breed (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, type_name TEXT NOT NULL, breed_name TEXT NOT NULL, CONSTRAINT uq_pet_breed_type_breed UNIQUE (type_name, breed_name))",
            "CREATE TABLE pet (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT, pet_breed_id BIGINT REFERENCES pet_breed(id), CONSTRAINT uq_pet_name_breed UNIQUE (name, pet_breed_id))",
            "CREATE TABLE customer (id BIGINT PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER, email TEXT, postal_code TEXT, country_id BIGINT REFERENCES country(id), pet_id BIGINT REFERENCES pet(id))",
            "CREATE TABLE seller (id BIGINT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT, postal_code TEXT, country_id BIGINT REFERENCES country(id))",
            "CREATE TABLE store (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT, location TEXT, phone TEXT, email TEXT, city_id BIGINT REFERENCES city(id), state_id BIGINT REFERENCES state(id), country_id BIGINT REFERENCES country(id), CONSTRAINT uq_store_name_location_city UNIQUE (name, location, city_id))",
            "CREATE TABLE fact_sales (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, sale_date DATE NOT NULL, customer_id BIGINT NOT NULL REFERENCES customer(id), seller_id BIGINT NOT NULL REFERENCES seller(id), product_id BIGINT NOT NULL REFERENCES product(id), store_id BIGINT NOT NULL REFERENCES store(id), quantity INTEGER NOT NULL, total_price NUMERIC(14,2) NOT NULL, unit_price NUMERIC(14,2))"
        ));
        }

        private static void writeNamedDimension(
            Dataset<Row> source,
            String sourceColumn,
            String tableName,
            String jdbcUrl,
            Properties jdbcProps
        ) {
            Dataset<Row> dim = source
                .select(col(sourceColumn).alias("name"))
                .where(col("name").isNotNull())
                .distinct();
            writeTable(dim, jdbcUrl, tableName, jdbcProps);
        }

        private static void writeTable(Dataset<Row> df, String jdbcUrl, String tableName, Properties jdbcProps) {
            df.write()
                .mode(SaveMode.Append)
                .jdbc(jdbcUrl, tableName, jdbcProps);
        }

        private static Dataset<Row> readTable(SparkSession spark, String jdbcUrl, String tableName, Properties jdbcProps) {
            return spark.read().jdbc(jdbcUrl, tableName, jdbcProps);
        }

        private static Properties buildJdbcProperties(String user, String password) {
            Properties jdbcProps = new Properties();
            jdbcProps.setProperty("user", user);
            jdbcProps.setProperty("password", password);
            jdbcProps.setProperty("driver", "org.postgresql.Driver");
            return jdbcProps;
        }

        private static void executeSqlBatch(String jdbcUrl, String user, String password, List<String> statements) throws SQLException {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                 Statement stmt = conn.createStatement()) {
                for (String sql : statements) {
                    stmt.execute(sql);
                }
            }
        }

    private static Column nullIfEmpty(Column c) {
        return when(c.equalTo(""), lit(null)).otherwise(c);
    }

    private static String getRequiredEnv(String key) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + key);
        }
        return val;
    }
}
