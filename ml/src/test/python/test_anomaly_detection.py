import os
import sys
import unittest

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "main",
        "python",
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from anomaly_detection import (
    calculate_residual,
    calculate_median,
    calculate_mad,
    calculate_modified_z_score,
    is_anomaly,
    detect_anomaly,
    detect_anomalies_by_product,
)


class AnomalyDetectionTest(unittest.TestCase):

    # ------------------------------------------------------------------
    # Residual
    # ------------------------------------------------------------------

    def test_calculate_residual_returns_actual_minus_predicted(self):
        residual = calculate_residual(
            actual=120.0,
            predicted=100.0,
        )

        self.assertEqual(20.0, residual)

    def test_calculate_residual_can_be_negative(self):
        residual = calculate_residual(
            actual=80.0,
            predicted=100.0,
        )

        self.assertEqual(-20.0, residual)

    def test_calculate_residual_returns_zero_when_actual_equals_predicted(self):
        residual = calculate_residual(
            actual=100.0,
            predicted=100.0,
        )

        self.assertEqual(0.0, residual)

    # ------------------------------------------------------------------
    # Median
    # ------------------------------------------------------------------

    def test_calculate_median_returns_middle_value_for_odd_number_of_items(self):
        result = calculate_median(
            [10.0, 20.0, 30.0]
        )

        self.assertEqual(20.0, result)

    def test_calculate_median_returns_average_of_middle_values_for_even_items(self):
        result = calculate_median(
            [10.0, 20.0, 30.0, 40.0]
        )

        self.assertEqual(25.0, result)

    def test_calculate_median_does_not_require_sorted_input(self):
        result = calculate_median(
            [30.0, 10.0, 20.0]
        )

        self.assertEqual(20.0, result)

    # ------------------------------------------------------------------
    # MAD
    # ------------------------------------------------------------------

    def test_calculate_mad_returns_median_absolute_deviation(self):
        mad = calculate_mad(
            [10.0, 20.0, 30.0]
        )

        self.assertEqual(10.0, mad)

    def test_calculate_mad_returns_zero_when_all_values_are_equal(self):
        mad = calculate_mad(
            [20.0, 20.0, 20.0]
        )

        self.assertEqual(0.0, mad)

    def test_calculate_mad_is_not_affected_by_input_order(self):
        mad = calculate_mad(
            [30.0, 10.0, 20.0]
        )

        self.assertEqual(10.0, mad)

    # ------------------------------------------------------------------
    # Modified Z-Score
    # ------------------------------------------------------------------

    def test_calculate_modified_z_score_returns_expected_positive_value(self):
        score = calculate_modified_z_score(
            residual=50.0,
            median_residual=20.0,
            mad=10.0,
        )

        self.assertAlmostEqual(
            2.0235,
            score,
            places=4,
        )

    def test_calculate_modified_z_score_returns_expected_negative_value(self):
        score = calculate_modified_z_score(
            residual=-10.0,
            median_residual=20.0,
            mad=10.0,
        )

        self.assertAlmostEqual(
            -2.0235,
            score,
            places=4,
        )

    def test_modified_z_score_returns_zero_when_residual_equals_median(self):
        score = calculate_modified_z_score(
            residual=20.0,
            median_residual=20.0,
            mad=10.0,
        )

        self.assertEqual(
            0.0,
            score,
        )

    def test_modified_z_score_returns_zero_when_mad_is_zero_and_residual_matches_median(
            self,
    ):
        score = calculate_modified_z_score(
            residual=20.0,
            median_residual=20.0,
            mad=0.0,
        )

        self.assertEqual(
            0.0,
            score,
        )

    def test_modified_z_score_marks_difference_when_mad_is_zero(self):
        score = calculate_modified_z_score(
            residual=30.0,
            median_residual=20.0,
            mad=0.0,
        )

        self.assertGreater(
            abs(score),
            3.5,
        )

    # ------------------------------------------------------------------
    # is_anomaly
    # ------------------------------------------------------------------

    def test_is_anomaly_returns_true_when_positive_score_exceeds_threshold(self):
        self.assertTrue(
            is_anomaly(3.6)
        )

    def test_is_anomaly_returns_true_when_negative_score_exceeds_threshold(self):
        self.assertTrue(
            is_anomaly(-3.6)
        )

    def test_is_anomaly_returns_false_below_positive_threshold(self):
        self.assertFalse(
            is_anomaly(3.4)
        )

    def test_is_anomaly_returns_false_below_negative_threshold(self):
        self.assertFalse(
            is_anomaly(-3.4)
        )

    def test_is_anomaly_returns_false_at_exact_positive_threshold(self):
        self.assertFalse(
            is_anomaly(3.5)
        )

    def test_is_anomaly_returns_false_at_exact_negative_threshold(self):
        self.assertFalse(
            is_anomaly(-3.5)
        )

    def test_is_anomaly_uses_custom_threshold(self):
        self.assertTrue(
            is_anomaly(
                score=2.1,
                threshold=2.0,
            )
        )

    def test_is_anomaly_returns_false_with_custom_threshold(self):
        self.assertFalse(
            is_anomaly(
                score=2.0,
                threshold=2.0,
            )
        )

    # ------------------------------------------------------------------
    # Complete anomaly detection
    # ------------------------------------------------------------------

    def test_detect_anomaly_returns_complete_result(self):
        result = detect_anomaly(
            actual=150.0,
            predicted=100.0,
            historical_residuals=[
                10.0,
                20.0,
                30.0,
            ],
        )

        self.assertEqual(
            150.0,
            result["actual"],
        )

        self.assertEqual(
            100.0,
            result["predicted"],
        )

        self.assertEqual(
            50.0,
            result["residual"],
        )

        self.assertEqual(
            20.0,
            result["median_residual"],
        )

        self.assertEqual(
            10.0,
            result["mad"],
        )

        self.assertAlmostEqual(
            2.0235,
            result["modified_z_score"],
            places=4,
        )

        self.assertFalse(
            result["is_anomaly"]
        )

    def test_detect_anomaly_flags_large_positive_deviation(self):
        result = detect_anomaly(
            actual=150.0,
            predicted=100.0,
            historical_residuals=[
                -2.0,
                0.0,
                2.0,
            ],
        )

        self.assertEqual(
            50.0,
            result["residual"],
        )

        self.assertTrue(
            result["is_anomaly"]
        )

        self.assertGreater(
            result["modified_z_score"],
            3.5,
        )

    def test_detect_anomaly_flags_large_negative_deviation(self):
        result = detect_anomaly(
            actual=50.0,
            predicted=100.0,
            historical_residuals=[
                -2.0,
                0.0,
                2.0,
            ],
        )

        self.assertEqual(
            -50.0,
            result["residual"],
        )

        self.assertTrue(
            result["is_anomaly"]
        )

        self.assertLess(
            result["modified_z_score"],
            -3.5,
        )

    def test_detect_anomaly_does_not_flag_expected_variation(self):
        result = detect_anomaly(
            actual=102.0,
            predicted=100.0,
            historical_residuals=[
                -2.0,
                0.0,
                2.0,
            ],
        )

        self.assertEqual(
            2.0,
            result["residual"],
        )

        self.assertFalse(
            result["is_anomaly"]
        )

    # ------------------------------------------------------------------
    # Product isolation
    # ------------------------------------------------------------------

    def test_detect_anomalies_by_product_keeps_product_histories_isolated(self):
        records = [
            {
                "product_id": "SKU-001",
                "actual": 150.0,
                "predicted": 100.0,
                "historical_residuals": [
                    -2.0,
                    0.0,
                    2.0,
                ],
            },
            {
                "product_id": "SKU-002",
                "actual": 102.0,
                "predicted": 100.0,
                "historical_residuals": [
                    -10.0,
                    0.0,
                    10.0,
                ],
            },
        ]

        results = detect_anomalies_by_product(
            records
        )

        self.assertEqual(
            2,
            len(results),
        )

        sku_001 = results[0]
        sku_002 = results[1]

        self.assertEqual(
            "SKU-001",
            sku_001["product_id"],
        )

        self.assertTrue(
            sku_001["is_anomaly"]
        )

        self.assertEqual(
            "SKU-002",
            sku_002["product_id"],
        )

        self.assertFalse(
            sku_002["is_anomaly"]
        )


if __name__ == "__main__":
    unittest.main()