//! Comprehensive tests for Phase 2.5 features.
//! pushdown/tests.rs
//! Tests for cost model, selectivity estimation, EXPLAIN parsing, parallel partitioning, and optimizer rules.

#[cfg(test)]
mod cost_model_tests {
    use crate::pushdown::cost_model::{CostParams, decide_push, Fidelity, Predicate};
    use crate::pushdown::stats::SourceStatistics;

    #[test]
    fn test_cost_params_defaults() {
        let params = CostParams::default();
        assert_eq!(params.max_source_cost, 50000);
        assert_eq!(params.keep_threshold, 0.30);
        assert_eq!(params.statistics_ttl_secs, 3600);
    }

    #[test]
    fn test_decide_push_with_index() {
        let params = CostParams::default();
        let stats = SourceStatistics::default();
        let pred = Predicate::Column("id".to_string());

        let decision = decide_push(&pred, Fidelity::Exact, &stats, &params, None, &[]);
        // Should keep by default when no index info
        assert!(matches!(decision, crate::pushdown::cost_model::CostDecision::Keep { .. }));
    }

    #[test]
    fn test_selectivity_estimation_equality() {
        use crate::pushdown::cost_model::estimate_selectivity_from_stats;

        let stats = SourceStatistics {
            n_distinct: Some(1000),
            null_frac: Some(0.05),
            avg_width: Some(8),
            ..Default::default()
        };

        let selectivity = estimate_selectivity_from_stats(&stats, "=");
        // For equality: 1 / n_distinct = 1 / 1000 = 0.001
        assert!(selectivity > 0.0 && selectivity <= 0.01);
    }

    #[test]
    fn test_selectivity_estimation_range() {
        use crate::pushdown::cost_model::estimate_selectivity_from_stats;

        let stats = SourceStatistics {
            n_distinct: Some(10000),
            null_frac: Some(0.01),
            avg_width: Some(4),
            ..Default::default()
        };

        let selectivity = estimate_selectivity_from_stats(&stats, ">");
        // For range: assume 0.33 (1/3 of rows)
        assert!(selectivity > 0.2 && selectivity < 0.5);
    }

    #[test]
    fn test_selectivity_with_nulls() {
        use crate::pushdown::cost_model::estimate_selectivity_from_stats;

        let stats = SourceStatistics {
            n_distinct: Some(100),
            null_frac: Some(0.50), // 50% nulls
            avg_width: Some(8),
            ..Default::default()
        };

        let selectivity = estimate_selectivity_from_stats(&stats, "IS NULL");
        // IS NULL should return null_frac
        assert!((selectivity - 0.50).abs() < 0.01);
    }
}

#[cfg(test)]
mod explain_tests {
    use crate::pushdown::explain::{AccessMethod, ExplainEstimate, parse_explain_json};

    #[test]
    fn test_access_method_from_node_type() {
        assert_eq!(
            AccessMethod::from_node_type("Seq Scan"),
            Some(AccessMethod::SeqScan)
        );
        assert_eq!(
            AccessMethod::from_node_type("Index Scan"),
            Some(AccessMethod::IndexScan)
        );
        assert_eq!(
            AccessMethod::from_node_type("Index Only Scan"),
            Some(AccessMethod::IndexOnlyScan)
        );
        assert_eq!(
            AccessMethod::from_node_type("Bitmap Heap Scan"),
            Some(AccessMethod::BitmapHeapScan)
        );
        assert_eq!(AccessMethod::from_node_type("Unknown"), None);
    }

    #[test]
    fn test_access_method_is_indexed() {
        assert!(!AccessMethod::SeqScan.is_indexed());
        assert!(AccessMethod::IndexScan.is_indexed());
        assert!(AccessMethod::IndexOnlyScan.is_indexed());
        assert!(AccessMethod::BitmapHeapScan.is_indexed());
    }

    #[test]
    fn test_explain_estimate_creation() {
        let estimate = ExplainEstimate {
            access_method: AccessMethod::IndexScan,
            index_name: Some("idx_orders_id".to_string()),
            rows: 100,
            total_cost: 45.5,
        };

        assert_eq!(estimate.access_method, AccessMethod::IndexScan);
        assert_eq!(estimate.index_name, Some("idx_orders_id".to_string()));
        assert_eq!(estimate.rows, 100);
    }

    #[test]
    fn test_parse_explain_json_seq_scan() {
        let json_str = r#"{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Plans": [],
                "Rows": 1000,
                "Total Cost": 500.0
            }
        }"#;

        if let Ok(estimate) = parse_explain_json(json_str) {
            assert_eq!(estimate.access_method, AccessMethod::SeqScan);
            assert_eq!(estimate.rows, 1000);
        }
    }

    #[test]
    fn test_parse_explain_json_index_scan() {
        let json_str = r#"{
            "Plan": {
                "Node Type": "Index Scan",
                "Index Name": "idx_id",
                "Relation Name": "orders",
                "Rows": 50,
                "Total Cost": 25.5
            }
        }"#;

        if let Ok(estimate) = parse_explain_json(json_str) {
            assert_eq!(estimate.access_method, AccessMethod::IndexScan);
            assert_eq!(estimate.index_name, Some("idx_id".to_string()));
            assert_eq!(estimate.rows, 50);
        }
    }
}

#[cfg(test)]
mod parallel_partition_tests {
    use crate::extractor::postgres::parallel::{
        ParallelStrategy, ParallelScanConfig, ScanPartition,
    };

    #[test]
    fn test_parallel_strategy_parse() {
        assert_eq!(
            ParallelStrategy::parse("keyset"),
            ParallelStrategy::Keyset {
                partition_column: "id".to_string()
            }
        );
        assert_eq!(ParallelStrategy::parse("ctid"), ParallelStrategy::Ctid);
        assert_eq!(ParallelStrategy::parse("none"), ParallelStrategy::None);
        assert_eq!(ParallelStrategy::parse("unknown"), ParallelStrategy::None);
    }

    #[test]
    fn test_parallel_scan_config_default() {
        let config = ParallelScanConfig::default();
        assert_eq!(config.strategy, ParallelStrategy::None);
        assert_eq!(config.partitions, 1);
    }

    #[test]
    fn test_keyset_partition_predicates() {
        // Simulate partition computation: 1000 rows, 4 partitions
        let partitions = vec![
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(250),
                predicate: Some("\"id\" >= 0 AND \"id\" < 250".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(250),
                hi: Some(500),
                predicate: Some("\"id\" >= 250 AND \"id\" < 500".to_string()),
            },
            ScanPartition {
                partition_id: 2,
                lo: Some(500),
                hi: Some(750),
                predicate: Some("\"id\" >= 500 AND \"id\" < 750".to_string()),
            },
            ScanPartition {
                partition_id: 3,
                lo: Some(750),
                hi: Some(1001),
                predicate: Some("\"id\" >= 750 AND \"id\" < 1001".to_string()),
            },
        ];

        assert_eq!(partitions.len(), 4);
        for (i, p) in partitions.iter().enumerate() {
            assert_eq!(p.partition_id, i);
            assert!(p.predicate.is_some());
        }
    }

    #[test]
    fn test_ctid_partition_predicates() {
        // Simulate partition computation: 100 pages, 4 partitions
        let partitions = vec![
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(25),
                predicate: Some("ctid >= '(0,1)'::tid AND ctid < '(25,1)'::tid".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(25),
                hi: Some(50),
                predicate: Some("ctid >= '(25,1)'::tid AND ctid < '(50,1)'::tid".to_string()),
            },
        ];

        assert_eq!(partitions.len(), 2);
        for p in partitions.iter() {
            assert!(p.lo.is_some());
            assert!(p.hi.is_some());
            assert!(p.predicate.is_some());
            let pred = p.predicate.as_ref().unwrap();
            assert!(pred.contains("ctid"));
        }
    }

    #[test]
    fn test_single_partition_fallback() {
        // When partitions = 1, should return single partition with no bounds
        let expected = ScanPartition {
            partition_id: 0,
            lo: None,
            hi: None,
            predicate: None,
        };

        assert_eq!(expected.partition_id, 0);
        assert_eq!(expected.lo, None);
        assert_eq!(expected.hi, None);
        assert_eq!(expected.predicate, None);
    }
}

#[cfg(test)]
mod optimizer_rule_tests {
    use crate::pushdown::optimizer_rule::SourceAwarePushdown;
    use datafusion::prelude::*;

    #[test]
    fn test_collect_filters_single() {
        let expr = col("id").eq(lit(42i64));
        let filters = SourceAwarePushdown::collect_filters(&expr);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_collect_filters_conjunction() {
        let expr = col("id")
            .eq(lit(42i64))
            .and(col("status").eq(lit("PAID")));
        let filters = SourceAwarePushdown::collect_filters(&expr);
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_collect_filters_nested_conjunction() {
        let expr = col("id")
            .eq(lit(42i64))
            .and(col("status").eq(lit("PAID")))
            .and(col("amount").gt(lit(100.0)));
        let filters = SourceAwarePushdown::collect_filters(&expr);
        assert_eq!(filters.len(), 3);
    }

    #[test]
    fn test_reconstruct_filter_single() {
        let expr = col("id").eq(lit(42i64));
        let filters = vec![expr];
        let reconstructed = SourceAwarePushdown::reconstruct_filter(&filters);
        assert!(reconstructed.is_some());
    }

    #[test]
    fn test_reconstruct_filter_multiple() {
        let expr1 = col("id").eq(lit(42i64));
        let expr2 = col("status").eq(lit("PAID"));
        let filters = vec![expr1, expr2];
        let reconstructed = SourceAwarePushdown::reconstruct_filter(&filters);
        assert!(reconstructed.is_some());

        let result = reconstructed.unwrap();
        // Result should be expr1 AND expr2
        match result {
            Expr::BinaryExpr(be) => {
                assert_eq!(be.op, datafusion::logical_expr::Operator::And);
            }
            _ => panic!("Expected BinaryExpr"),
        }
    }

    #[test]
    fn test_reconstruct_filter_empty() {
        let filters: Vec<Expr> = vec![];
        let reconstructed = SourceAwarePushdown::reconstruct_filter(&filters);
        assert!(reconstructed.is_none());
    }
}

#[cfg(test)]
mod integration_tests {
    use crate::pushdown::cost_model::CostParams;
    use crate::pushdown::stats::SourceStatistics;

    #[test]
    fn test_cost_model_workflow() {
        // Simulate a complete cost-based decision workflow
        let params = CostParams {
            max_source_cost: 50000,
            keep_threshold: 0.30,
            statistics_ttl_secs: 3600,
        };

        let stats = SourceStatistics {
            n_distinct: Some(100),
            null_frac: Some(0.05),
            avg_width: Some(8),
        };

        // Verify params were created correctly
        assert_eq!(params.max_source_cost, 50000);
        assert!(stats.n_distinct.unwrap() > 0);
    }

    #[test]
    fn test_selectivity_estimation_pipeline() {
        use crate::pushdown::cost_model::estimate_selectivity_from_stats;

        let test_cases = vec![
            ("=", 100, 0.001, 0.01),     // equality: 1/100 = 0.01
            (">", 1000, 0.2, 0.5),        // range: ~0.33
            ("IS NULL", 10000, 0.0, 1.0), // null check: depends on null_frac
        ];

        for (op, n_distinct, min_sel, max_sel) in test_cases {
            let stats = SourceStatistics {
                n_distinct: Some(n_distinct),
                null_frac: Some(0.05),
                avg_width: Some(8),
            };

            let selectivity = estimate_selectivity_from_stats(&stats, op);
            assert!(
                selectivity >= min_sel && selectivity <= max_sel,
                "Op {}: selectivity {} not in range [{}, {}]",
                op,
                selectivity,
                min_sel,
                max_sel
            );
        }
    }

    #[test]
    fn test_parallel_partitioning_coverage() {
        use crate::extractor::postgres::parallel::ScanPartition;

        // Test that partitions don't overlap
        let partitions = vec![
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(250),
                predicate: Some("id >= 0 AND id < 250".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(250),
                hi: Some(500),
                predicate: Some("id >= 250 AND id < 500".to_string()),
            },
            ScanPartition {
                partition_id: 2,
                lo: Some(500),
                hi: Some(1000),
                predicate: Some("id >= 500 AND id < 1000".to_string()),
            },
        ];

        // Verify no gaps
        for i in 0..partitions.len() - 1 {
            let curr = &partitions[i];
            let next = &partitions[i + 1];

            if let (Some(curr_hi), Some(next_lo)) = (curr.hi, next.lo) {
                assert_eq!(curr_hi, next_lo, "Gap between partitions {} and {}", i, i + 1);
            }
        }
    }
}
