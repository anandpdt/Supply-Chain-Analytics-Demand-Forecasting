
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hourly_supply_chain_pipeline",
    schedule="@hourly",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["supply-chain", "automation"],
)
def hourly_supply_chain_pipeline():

    # ============================================
    # TASK 1: VALIDATE SOURCE DATA
    # ============================================

    @task
    def check_source_data():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        result = hook.get_first("""
            SELECT
                (SELECT COUNT(*)
                 FROM dataco.order_items),

                (SELECT COUNT(*)
                 FROM ops.inventory_ledger_daily)
        """)

        print("Order items:", result[0])
        print("Inventory ledger records:", result[1])

        if result[0] != 180519:
            raise ValueError(
                "DataCo record count mismatch"
            )

        if result[1] != 32568:
            raise ValueError(
                "Inventory record count mismatch"
            )

        print("Source validation PASSED!")

    # ============================================
    # TASK 2: ADVANCE HOURLY INVENTORY SIMULATION
    # ============================================

    @task
    def advance_inventory_simulation():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        result = hook.run(
            sql="""
                SELECT
                    simulated_hour,
                    inventory_rows,
                    units_received,
                    shipments_created,
                    already_processed
                FROM ops.advance_simulated_hour();
            """,
            autocommit=False,
            handler=lambda cursor: cursor.fetchone()
        )

        if result is None:
            raise ValueError(
                "No simulation result returned"
            )

        sim_hour, rows, receipts, orders, repeated = result

        print("================================")
        print("HOURLY INVENTORY SIMULATION")
        print("================================")

        print("Simulated hour:", sim_hour)
        print("Inventory rows:", rows)
        print("Units received:", receipts)
        print("Shipments created:", orders)
        print("Already processed:", repeated)

        if rows != 354:
            raise ValueError(
                f"Expected 354 inventory rows, got {rows}"
            )

        if repeated:
            print(
                "This execution hour was already processed."
            )
            print(
                "No duplicate inventory changes applied."
            )
        else:
            print(
                "New simulated inventory hour created!"
            )

        print("Inventory simulation PASSED!")

    # ============================================
    # TASK 3: REFRESH KPI SNAPSHOT
    # ============================================

    # TASK: SAVE DYNAMIC OPERATIONAL KPIS

    @task
    def save_operational_kpis():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        hook.run("""
            INSERT INTO ops.hourly_operational_kpi_snapshots (
                simulated_hour,
                total_demand,
                fulfilled_units,
                unmet_units,
                closing_stock,
                stockout_rate_percent,
                fulfillment_rate_percent,
                projected_days_of_inventory,
                captured_at
            )

            SELECT
                snapshot_hour,
                total_demand,
                fulfilled_units,
                unmet_units,
                closing_stock,
                stockout_rate_percent,
                fulfillment_rate_percent,
                projected_days_of_inventory,
                NOW()

            FROM ops.v_live_hourly_kpis

            ORDER BY snapshot_hour DESC
            LIMIT 1

            ON CONFLICT (simulated_hour)
            DO UPDATE SET
                total_demand = EXCLUDED.total_demand,
                fulfilled_units = EXCLUDED.fulfilled_units,
                unmet_units = EXCLUDED.unmet_units,
                closing_stock = EXCLUDED.closing_stock,
                stockout_rate_percent =
                    EXCLUDED.stockout_rate_percent,
                fulfillment_rate_percent =
                    EXCLUDED.fulfillment_rate_percent,
                projected_days_of_inventory =
                    EXCLUDED.projected_days_of_inventory,
                captured_at = NOW();
        """)

        print("Dynamic operational KPI snapshot saved!")
    
    # ============================================
    # TASK 6: MONITOR ANOMALY RESULTS
    # ============================================

    @task
    def monitor_anomalies():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        # Ensure saved anomaly records satisfy
        # our detection and severity rules.
        validation = hook.get_first("""
            SELECT
                (SELECT COUNT(*)
                 FROM analytics.demand_spike_anomalies
                 WHERE z_score < 3
                    OR demand_units < 3
                    OR baseline_std <= 0),

                (SELECT COUNT(*)
                 FROM analytics.v_supplier_delay_anomalies
                 WHERE delay_days < 2
                    OR (delay_days = 2
                        AND anomaly_severity <> 'WARNING')
                    OR (delay_days >= 3
                        AND anomaly_severity <> 'CRITICAL'))
        """)

        if validation is None or any(validation):
            raise ValueError(
                f"Anomaly validation failed: {validation}"
            )

        # Save monitoring counts once per clock hour.
        hook.run("""
            INSERT INTO analytics.hourly_anomaly_snapshots (
                snapshot_hour,
                historical_demand_spikes,
                simulated_supplier_delays,
                supplier_warnings,
                supplier_critical,
                checked_at
            )

            SELECT
                date_trunc('hour', NOW()),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'DEMAND_SPIKE'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                      AND severity = 'WARNING'
                ),

                COUNT(*) FILTER (
                    WHERE anomaly_type = 'SUPPLIER_DELAY'
                      AND severity = 'CRITICAL'
                ),

                NOW()

            FROM analytics.v_control_tower_anomalies

            ON CONFLICT (snapshot_hour)
            DO UPDATE SET
                historical_demand_spikes =
                    EXCLUDED.historical_demand_spikes,
                simulated_supplier_delays =
                    EXCLUDED.simulated_supplier_delays,
                supplier_warnings =
                    EXCLUDED.supplier_warnings,
                supplier_critical =
                    EXCLUDED.supplier_critical,
                checked_at = NOW();
        """)

        result = hook.get_first("""
            SELECT
                historical_demand_spikes,
                simulated_supplier_delays,
                supplier_warnings,
                supplier_critical
            FROM analytics.hourly_anomaly_snapshots
            ORDER BY snapshot_hour DESC
            LIMIT 1;
        """)

        if result is None:
            raise ValueError(
                "Anomaly monitoring snapshot missing"
            )

        print("Historical demand spikes:", result[0])
        print("Simulated supplier delays:", result[1])
        print("Warning alerts:", result[2])
        print("Critical alerts:", result[3])
        print("Anomaly monitoring PASSED!")



    @task
    def refresh_kpi_snapshot():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        hook.run("""
            INSERT INTO ops.hourly_kpi_snapshots (
                snapshot_hour,
                stockout_rate,
                customer_shipping_pct,
                supplier_on_time_pct,
                avg_supplier_lead_days,
                days_of_inventory,
                inventory_as_of,
                source_note
            )
            SELECT
                date_trunc('hour', NOW()),
                stockout_rate,
                customer_shipping_pct,
                supplier_on_time_pct,
                avg_supplier_lead_days,
                days_of_inventory,
                inventory_as_of,
                'Historical DataCo + simulated operations'
            FROM ops.v_kpi_overview

            ON CONFLICT (snapshot_hour)
            DO UPDATE SET
                stockout_rate = EXCLUDED.stockout_rate,
                customer_shipping_pct = EXCLUDED.customer_shipping_pct,
                supplier_on_time_pct = EXCLUDED.supplier_on_time_pct,
                avg_supplier_lead_days = EXCLUDED.avg_supplier_lead_days,
                days_of_inventory = EXCLUDED.days_of_inventory,
                inventory_as_of = EXCLUDED.inventory_as_of,
                source_note = EXCLUDED.source_note,
                refreshed_at = NOW();
        """)

        print("Hourly KPI snapshot saved!")

    # ============================================
    # TASK 4: VERIFY KPI REFRESH
    # ============================================

    @task
    def verify_refresh():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(
            postgres_conn_id="supply_chain_postgres"
        )

        result = hook.get_first("""
            SELECT
                COUNT(*),
                MAX(refreshed_at)
            FROM ops.hourly_kpi_snapshots
            WHERE snapshot_hour =
                date_trunc('hour', NOW());
        """)

        if result is None or result[0] != 1:
            raise ValueError(
                "Hourly KPI snapshot missing"
            )

        print("Last KPI refresh:", result[1])
        print("Hourly pipeline PASSED!")

    # ============================================
    # DEFINE TASK EXECUTION ORDER
    # ============================================

    
    
    check = check_source_data()
    simulate = advance_inventory_simulation()
    operational_kpi = save_operational_kpis()
    anomalies = monitor_anomalies()
    refresh = refresh_kpi_snapshot()
    verify = verify_refresh()

    check >> simulate >> operational_kpi >> anomalies >> refresh >> verify


hourly_supply_chain_pipeline()
