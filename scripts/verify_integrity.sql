-- Script de Verificação de Integridade Referencial
-- Verifica se todas as foreign keys na tabela fato estão corretamente associadas às dimensões

\echo '========================================'
\echo 'VERIFICAÇÃO DE INTEGRIDADE REFERENCIAL'
\echo '========================================'
\echo ''

-- 1. Verificar time_id
\echo '1. Verificando time_id...'
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ OK: Todos os time_id estão associados'
        ELSE '✗ ERRO: ' || COUNT(*) || ' registros com time_id inválido'
    END as status
FROM analytics.fact_orders fo
LEFT JOIN analytics.dim_time dt ON fo.time_id = dt.time_id
WHERE fo.time_id IS NOT NULL AND dt.time_id IS NULL;

SELECT 
    COUNT(*) as total_pedidos,
    COUNT(fo.time_id) as pedidos_com_time_id,
    COUNT(*) - COUNT(fo.time_id) as pedidos_sem_time_id
FROM analytics.fact_orders fo;
\echo ''

-- 2. Verificar customer_key
\echo '2. Verificando customer_key...'
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ OK: Todos os customer_key estão associados'
        ELSE '✗ ERRO: ' || COUNT(*) || ' registros com customer_key inválido'
    END as status
FROM analytics.fact_orders fo
LEFT JOIN analytics.dim_customers dc ON fo.customer_key = dc.customer_key
WHERE fo.customer_key IS NOT NULL AND dc.customer_key IS NULL;

SELECT 
    COUNT(*) as total_pedidos,
    COUNT(fo.customer_key) as pedidos_com_customer_key,
    COUNT(*) - COUNT(fo.customer_key) as pedidos_sem_customer_key
FROM analytics.fact_orders fo;
\echo ''

-- 3. Verificar product_key
\echo '3. Verificando product_key...'
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ OK: Todos os product_key estão associados ou são NULL'
        ELSE '✗ ERRO: ' || COUNT(*) || ' registros com product_key inválido'
    END as status
FROM analytics.fact_orders fo
LEFT JOIN analytics.dim_products dp ON fo.product_key = dp.product_key
WHERE fo.product_key IS NOT NULL AND dp.product_key IS NULL;

SELECT 
    COUNT(*) as total_pedidos,
    COUNT(fo.product_key) as pedidos_com_product_key,
    COUNT(*) - COUNT(fo.product_key) as pedidos_sem_product_key
FROM analytics.fact_orders fo;
\echo ''

-- 4. Verificar seller_key
\echo '4. Verificando seller_key...'
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ OK: Todos os seller_key estão associados ou são NULL'
        ELSE '✗ ERRO: ' || COUNT(*) || ' registros com seller_key inválido'
    END as status
FROM analytics.fact_orders fo
LEFT JOIN analytics.dim_sellers ds ON fo.seller_key = ds.seller_key
WHERE fo.seller_key IS NOT NULL AND ds.seller_key IS NULL;

SELECT 
    COUNT(*) as total_pedidos,
    COUNT(fo.seller_key) as pedidos_com_seller_key,
    COUNT(*) - COUNT(fo.seller_key) as pedidos_sem_seller_key
FROM analytics.fact_orders fo;
\echo ''

-- 5. Verificar geography_key
\echo '5. Verificando geography_key...'
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ OK: Todos os geography_key estão associados ou são NULL'
        ELSE '✗ ERRO: ' || COUNT(*) || ' registros com geography_key inválido'
    END as status
FROM analytics.fact_orders fo
LEFT JOIN analytics.dim_geography dg ON fo.geography_key = dg.geography_key
WHERE fo.geography_key IS NOT NULL AND dg.geography_key IS NULL;

SELECT 
    COUNT(*) as total_pedidos,
    COUNT(fo.geography_key) as pedidos_com_geography_key,
    COUNT(*) - COUNT(fo.geography_key) as pedidos_sem_geography_key
FROM analytics.fact_orders fo;
\echo ''

-- 6. Verificar se main_product_id existe em dim_products
\echo '6. Verificando se main_product_id da fact_table existe em dim_products...'
\echo '   (Esta verificação requer acesso aos dados transformados)'
\echo ''

-- 7. Verificar se main_seller_id existe em dim_sellers
\echo '7. Verificando se main_seller_id da fact_table existe em dim_sellers...'
\echo '   (Esta verificação requer acesso aos dados transformados)'
\echo ''

-- 8. Resumo geral
\echo '========================================'
\echo 'RESUMO GERAL'
\echo '========================================'
SELECT 
    'Total de pedidos' as metrica,
    COUNT(*)::text as valor
FROM analytics.fact_orders
UNION ALL
SELECT 
    'Pedidos com time_id',
    COUNT(time_id)::text
FROM analytics.fact_orders
WHERE time_id IS NOT NULL
UNION ALL
SELECT 
    'Pedidos com customer_key',
    COUNT(customer_key)::text
FROM analytics.fact_orders
WHERE customer_key IS NOT NULL
UNION ALL
SELECT 
    'Pedidos com product_key',
    COUNT(product_key)::text
FROM analytics.fact_orders
WHERE product_key IS NOT NULL
UNION ALL
SELECT 
    'Pedidos com seller_key',
    COUNT(seller_key)::text
FROM analytics.fact_orders
WHERE seller_key IS NOT NULL
UNION ALL
SELECT 
    'Pedidos com geography_key',
    COUNT(geography_key)::text
FROM analytics.fact_orders
WHERE geography_key IS NOT NULL;

\echo ''
\echo '========================================'
\echo 'VERIFICAÇÃO CONCLUÍDA'
\echo '========================================'
