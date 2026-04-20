BEGIN;

INSERT INTO country (name)
SELECT DISTINCT country_name
FROM (
    SELECT customer_country AS country_name FROM public.mock_data_all
    UNION
    SELECT seller_country FROM public.mock_data_all
    UNION
    SELECT store_country FROM public.mock_data_all
    UNION
    SELECT supplier_country FROM public.mock_data_all
) t
WHERE country_name IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO state (name, country_id)
SELECT DISTINCT
    NULLIF(m.store_state, '') AS state_name,
    c.id
FROM public.mock_data_all m
JOIN country c ON c.name = m.store_country
WHERE NULLIF(m.store_state, '') IS NOT NULL
ON CONFLICT (name, country_id) DO NOTHING;

INSERT INTO city (name, state_id, country_id)
SELECT city_name, state_id, country_id
FROM (
    SELECT DISTINCT
        m.store_city AS city_name,
        st.id AS state_id,
        c.id AS country_id
    FROM public.mock_data_all m
    JOIN country c ON c.name = m.store_country
    LEFT JOIN state st
        ON st.name = NULLIF(m.store_state, '')
       AND st.country_id = c.id
    WHERE m.store_city IS NOT NULL

    UNION

    SELECT DISTINCT
        m.supplier_city AS city_name,
        NULL::BIGINT AS state_id,
        c.id AS country_id
    FROM public.mock_data_all m
    JOIN country c ON c.name = m.supplier_country
    WHERE m.supplier_city IS NOT NULL
) u
ON CONFLICT (name, state_id, country_id) DO NOTHING;

INSERT INTO product_category (name)
SELECT DISTINCT product_category
FROM public.mock_data_all
WHERE product_category IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO product_brand (name)
SELECT DISTINCT product_brand
FROM public.mock_data_all
WHERE product_brand IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO product_material (name)
SELECT DISTINCT product_material
FROM public.mock_data_all
WHERE product_material IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO product_color (name)
SELECT DISTINCT product_color
FROM public.mock_data_all
WHERE product_color IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO product_size (name)
SELECT DISTINCT product_size
FROM public.mock_data_all
WHERE product_size IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO pet_category (name)
SELECT DISTINCT pet_category
FROM public.mock_data_all
WHERE pet_category IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO supplier (name, contact, email, phone, address, city_id)
SELECT DISTINCT
    m.supplier_name,
    m.supplier_contact,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_address,
    ci.id
FROM public.mock_data_all m
LEFT JOIN country co ON co.name = m.supplier_country
LEFT JOIN city ci
    ON ci.name = m.supplier_city
   AND ci.country_id = co.id
   AND ci.state_id IS NULL
WHERE m.supplier_name IS NOT NULL
ON CONFLICT (name, email, phone) DO NOTHING;

INSERT INTO product (
    id,
    name,
    pet_category_id,
    category_id,
    brand_id,
    material_id,
    color_id,
    size_id,
    supplier_id,
    quantity,
    weight,
    rating,
    reviews,
    release_date,
    expiry_date,
    description
)
SELECT
    m.sale_product_id,
    m.product_name,
    pcg.id,
    pc.id,
    pb.id,
    pm.id,
    pco.id,
    ps.id,
    sup.id,
    m.product_quantity,
    m.product_weight,
    m.product_rating,
    m.product_reviews,
    TO_DATE(m.product_release_date, 'MM/DD/YYYY'),
    TO_DATE(m.product_expiry_date, 'MM/DD/YYYY'),
    m.product_description
FROM (
    SELECT DISTINCT ON (sale_product_id)
        sale_product_id,
        product_name,
        pet_category,
        product_category,
        product_brand,
        product_material,
        product_color,
        product_size,
        supplier_name,
        supplier_email,
        supplier_phone,
        product_quantity,
        product_weight,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date,
        product_description,
        id
    FROM public.mock_data_all
    WHERE sale_product_id IS NOT NULL
    ORDER BY sale_product_id, id
) m
LEFT JOIN pet_category pcg ON pcg.name = m.pet_category
LEFT JOIN product_category pc ON pc.name = m.product_category
LEFT JOIN product_brand pb ON pb.name = m.product_brand
LEFT JOIN product_material pm ON pm.name = m.product_material
LEFT JOIN product_color pco ON pco.name = m.product_color
LEFT JOIN product_size ps ON ps.name = m.product_size
LEFT JOIN supplier sup
    ON sup.name = m.supplier_name
   AND sup.email IS NOT DISTINCT FROM m.supplier_email
   AND sup.phone IS NOT DISTINCT FROM m.supplier_phone
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    pet_category_id = EXCLUDED.pet_category_id,
    category_id = EXCLUDED.category_id,
    brand_id = EXCLUDED.brand_id,
    material_id = EXCLUDED.material_id,
    color_id = EXCLUDED.color_id,
    size_id = EXCLUDED.size_id,
    supplier_id = EXCLUDED.supplier_id,
    quantity = EXCLUDED.quantity,
    weight = EXCLUDED.weight,
    rating = EXCLUDED.rating,
    reviews = EXCLUDED.reviews,
    release_date = EXCLUDED.release_date,
    expiry_date = EXCLUDED.expiry_date,
    description = EXCLUDED.description;

INSERT INTO pet_breed (type_name, breed_name)
SELECT DISTINCT
    m.customer_pet_type,
    m.customer_pet_breed
FROM public.mock_data_all m
WHERE m.customer_pet_type IS NOT NULL
  AND m.customer_pet_breed IS NOT NULL
ON CONFLICT (type_name, breed_name) DO NOTHING;

INSERT INTO pet (name, pet_breed_id)
SELECT DISTINCT
    m.customer_pet_name,
    pb.id
FROM public.mock_data_all m
JOIN pet_breed pb
    ON pb.type_name = m.customer_pet_type
   AND pb.breed_name = m.customer_pet_breed
WHERE m.customer_pet_name IS NOT NULL
ON CONFLICT (name, pet_breed_id) DO NOTHING;

INSERT INTO customer (
    id,
    first_name,
    last_name,
    age,
    email,
    postal_code,
    country_id,
    pet_id
)
SELECT
    s.sale_customer_id,
    s.customer_first_name,
    s.customer_last_name,
    s.customer_age,
    s.customer_email,
    NULLIF(s.customer_postal_code, ''),
    c.id,
    p.id
FROM (
    SELECT DISTINCT ON (sale_customer_id)
        sale_customer_id,
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_email,
        customer_postal_code,
        customer_country,
        customer_pet_name,
        customer_pet_type,
        customer_pet_breed,
        id
    FROM public.mock_data_all
    ORDER BY sale_customer_id, id
) s
LEFT JOIN country c ON c.name = s.customer_country
LEFT JOIN pet_breed pb
    ON pb.type_name = s.customer_pet_type
   AND pb.breed_name = s.customer_pet_breed
LEFT JOIN pet p
    ON p.name = s.customer_pet_name
   AND p.pet_breed_id = pb.id
WHERE s.sale_customer_id IS NOT NULL
ON CONFLICT (id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    age = EXCLUDED.age,
    email = EXCLUDED.email,
    postal_code = EXCLUDED.postal_code,
    country_id = EXCLUDED.country_id,
    pet_id = EXCLUDED.pet_id;

INSERT INTO seller (
    id,
    first_name,
    last_name,
    email,
    postal_code,
    country_id
)
SELECT
    s.sale_seller_id,
    s.seller_first_name,
    s.seller_last_name,
    s.seller_email,
    NULLIF(s.seller_postal_code, ''),
    c.id
FROM (
    SELECT DISTINCT ON (sale_seller_id)
        sale_seller_id,
        seller_first_name,
        seller_last_name,
        seller_email,
        seller_postal_code,
        seller_country,
        id
    FROM public.mock_data_all
    ORDER BY sale_seller_id, id
) s
LEFT JOIN country c ON c.name = s.seller_country
WHERE s.sale_seller_id IS NOT NULL
ON CONFLICT (id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    postal_code = EXCLUDED.postal_code,
    country_id = EXCLUDED.country_id;

INSERT INTO store (
    name,
    location,
    phone,
    email,
    city_id,
    state_id,
    country_id
)
SELECT DISTINCT
    m.store_name,
    m.store_location,
    m.store_phone,
    m.store_email,
    ci.id,
    st.id,
    co.id
FROM public.mock_data_all m
LEFT JOIN country co ON co.name = m.store_country
LEFT JOIN state st
    ON st.name = NULLIF(m.store_state, '')
   AND st.country_id = co.id
LEFT JOIN city ci
    ON ci.name = m.store_city
   AND ci.country_id = co.id
   AND ci.state_id IS NOT DISTINCT FROM st.id
WHERE m.store_name IS NOT NULL
ON CONFLICT (name, location, city_id) DO NOTHING;

INSERT INTO fact_sales (
    sale_date,
    customer_id,
    seller_id,
    product_id,
    store_id,
    quantity,
    total_price,
    unit_price
)
SELECT
    TO_DATE(m.sale_date, 'MM/DD/YYYY'),
    m.sale_customer_id,
    m.sale_seller_id,
    m.sale_product_id,
    st.id,
    m.sale_quantity,
    m.sale_total_price,
    m.product_price
FROM public.mock_data_all m
JOIN country co ON co.name = m.store_country
LEFT JOIN state sta
    ON sta.name = NULLIF(m.store_state, '')
   AND sta.country_id = co.id
JOIN city ci
    ON ci.name = m.store_city
   AND ci.country_id = co.id
   AND ci.state_id IS NOT DISTINCT FROM sta.id
JOIN store st
    ON st.name = m.store_name
   AND st.location IS NOT DISTINCT FROM m.store_location
   AND st.city_id = ci.id
WHERE TO_DATE(m.sale_date, 'MM/DD/YYYY') IS NOT NULL
  AND m.sale_customer_id IS NOT NULL
  AND m.sale_seller_id IS NOT NULL
  AND m.sale_product_id IS NOT NULL
  AND m.sale_quantity IS NOT NULL
  AND m.sale_total_price IS NOT NULL;

COMMIT;
