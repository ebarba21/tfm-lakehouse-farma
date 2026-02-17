"""
Consultas SQL y KPIs sobre el modelo dimensional Gold.

Demuestra el uso del modelo estrella para análisis de ventas,
rendimiento comercial y comparativa histórico vs streaming.
"""

from pyspark.sql import SparkSession


GOLD_PATH = "/data/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("gold_kpis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def register_tables(spark):
    """Registra tablas Gold como vistas temporales para SQL."""
    tables = [
        "dim_time", "dim_product", "dim_customer",
        "dim_sales_rep", "fact_sales", "fact_orders"
    ]
    for table in tables:
        path = f"{GOLD_PATH}/{table}"
        spark.read.format("delta").load(path).createOrReplaceTempView(table)
        print(f"[kpi] Registrada tabla: {table}")


def run_kpi(spark, name, query):
    print(f"\n{'='*70}")
    print(f"KPI: {name}")
    print("="*70)
    print(f"SQL:\n{query.strip()}")
    print("-"*70)
    spark.sql(query).show(20, truncate=False)


def main():
    print("=" * 70)
    print("[kpi] CONSULTAS SQL Y KPIs - MODELO GOLD")
    print("=" * 70)

    spark = create_spark_session()

    try:
        register_tables(spark)

        # -- Ventas totales por canal --
        run_kpi(spark, "VENTAS TOTALES POR CANAL", """
            SELECT
                channel,
                COUNT(*) as num_transacciones,
                SUM(qty) as unidades_vendidas,
                ROUND(SUM(sales), 2) as ventas_totales,
                ROUND(AVG(sales), 2) as ticket_medio
            FROM fact_sales
            GROUP BY channel
            ORDER BY ventas_totales DESC
        """)

        # -- Ventas por país --
        run_kpi(spark, "VENTAS POR PAÍS", """
            SELECT
                country,
                COUNT(*) as num_transacciones,
                ROUND(SUM(sales), 2) as ventas_totales,
                ROUND(AVG(sales), 2) as ticket_medio,
                COUNT(DISTINCT city) as ciudades_activas
            FROM fact_sales
            GROUP BY country
            ORDER BY ventas_totales DESC
        """)

        # -- Top 10 productos --
        run_kpi(spark, "TOP 10 PRODUCTOS POR VENTAS", """
            SELECT
                p.product_name,
                p.product_class,
                COUNT(*) as num_transacciones,
                SUM(f.qty) as unidades_vendidas,
                ROUND(SUM(f.sales), 2) as ventas_totales
            FROM fact_sales f
            JOIN dim_product p ON f.product_sk = p.product_sk
            GROUP BY p.product_name, p.product_class
            ORDER BY ventas_totales DESC
            LIMIT 10
        """)

        run_kpi(spark, "VENTAS POR CLASE DE PRODUCTO", """
            SELECT
                p.product_class,
                COUNT(DISTINCT p.product_name) as num_productos,
                COUNT(*) as num_transacciones,
                ROUND(SUM(f.sales), 2) as ventas_totales,
                ROUND(AVG(f.sales), 2) as ticket_medio
            FROM fact_sales f
            JOIN dim_product p ON f.product_sk = p.product_sk
            GROUP BY p.product_class
            ORDER BY ventas_totales DESC
        """)

        run_kpi(spark, "TOP 10 SALES REPS POR VENTAS", """
            SELECT
                sr.sales_rep_name,
                sr.manager,
                sr.sales_team,
                COUNT(*) as num_transacciones,
                ROUND(SUM(f.sales), 2) as ventas_totales,
                ROUND(AVG(f.sales), 2) as ticket_medio
            FROM fact_sales f
            JOIN dim_sales_rep sr ON f.sales_rep_sk = sr.sales_rep_sk
            GROUP BY sr.sales_rep_name, sr.manager, sr.sales_team
            ORDER BY ventas_totales DESC
            LIMIT 10
        """)

        run_kpi(spark, "RENDIMIENTO POR EQUIPO DE VENTAS", """
            SELECT
                sr.sales_team,
                COUNT(DISTINCT sr.sales_rep_name) as num_reps,
                COUNT(*) as num_transacciones,
                ROUND(SUM(f.sales), 2) as ventas_totales,
                ROUND(SUM(f.sales) / COUNT(DISTINCT sr.sales_rep_name), 2) as ventas_por_rep
            FROM fact_sales f
            JOIN dim_sales_rep sr ON f.sales_rep_sk = sr.sales_rep_sk
            GROUP BY sr.sales_team
            ORDER BY ventas_totales DESC
        """)

        run_kpi(spark, "EVOLUCIÓN DE VENTAS POR AÑO Y TRIMESTRE", """
            SELECT
                t.year,
                t.quarter_name,
                COUNT(*) as num_transacciones,
                ROUND(SUM(f.sales), 2) as ventas_totales
            FROM fact_sales f
            JOIN dim_time t ON f.time_sk = t.time_sk
            GROUP BY t.year, t.quarter_name, t.quarter
            ORDER BY t.year, t.quarter
        """)

        run_kpi(spark, "TOP 10 CLIENTES POR VOLUMEN DE COMPRA", """
            SELECT
                c.customer_name,
                c.city,
                c.country,
                c.channel,
                COUNT(*) as num_transacciones,
                ROUND(SUM(f.sales), 2) as compras_totales
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_sk = c.customer_sk
            GROUP BY c.customer_name, c.city, c.country, c.channel
            ORDER BY compras_totales DESC
            LIMIT 10
        """)

        run_kpi(spark, "RESUMEN PEDIDOS EN TIEMPO REAL (STREAMING)", """
            SELECT
                channel,
                country,
                COUNT(*) as num_pedidos,
                SUM(qty) as unidades_pedidas,
                ROUND(SUM(total_amount), 2) as valor_total,
                ROUND(AVG(total_amount), 2) as pedido_medio
            FROM fact_orders
            GROUP BY channel, country
            ORDER BY valor_total DESC
        """)

        # Comparativa entre datos históricos y streaming
        run_kpi(spark, "COMPARATIVA: PRODUCTOS EN HISTÓRICO VS STREAMING", """
            WITH ventas_historicas AS (
                SELECT
                    p.product_name,
                    SUM(f.sales) as ventas_historicas
                FROM fact_sales f
                JOIN dim_product p ON f.product_sk = p.product_sk
                GROUP BY p.product_name
            ),
            pedidos_streaming AS (
                SELECT
                    p.product_name,
                    SUM(o.total_amount) as pedidos_streaming
                FROM fact_orders o
                JOIN dim_product p ON o.product_sk = p.product_sk
                GROUP BY p.product_name
            )
            SELECT
                COALESCE(h.product_name, s.product_name) as product_name,
                ROUND(COALESCE(h.ventas_historicas, 0), 2) as ventas_historicas,
                ROUND(COALESCE(s.pedidos_streaming, 0), 2) as pedidos_streaming
            FROM ventas_historicas h
            FULL OUTER JOIN pedidos_streaming s
                ON h.product_name = s.product_name
            WHERE s.pedidos_streaming IS NOT NULL
            ORDER BY pedidos_streaming DESC
            LIMIT 15
        """)

        # Resumen ejecutivo
        print("\n" + "=" * 70)
        print("RESUMEN EJECUTIVO")
        print("=" * 70)

        summary = spark.sql("""
            SELECT
                'Ventas Históricas' as fuente,
                COUNT(*) as transacciones,
                ROUND(SUM(sales), 2) as valor_total
            FROM fact_sales
            UNION ALL
            SELECT
                'Pedidos Streaming' as fuente,
                COUNT(*) as transacciones,
                ROUND(SUM(total_amount), 2) as valor_total
            FROM fact_orders
        """)
        summary.show(truncate=False)

        print("=" * 70)
        print("[kpi] FIN - Consultas ejecutadas exitosamente")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()