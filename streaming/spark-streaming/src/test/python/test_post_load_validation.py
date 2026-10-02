import os
import sys
import unittest


CURRENT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(
        CURRENT_DIR,
        "..",
        "..",
        "main",
        "python",
    )
)

if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(
        0,
        MAIN_PYTHON_DIR
    )

from post_load_validation import (
    validate_pipeline_counts,
)


class PostLoadValidationTest(
    unittest.TestCase
):

    def test_matching_counts_pass(
        self
    ):
        validate_pipeline_counts(
            silver_count=5,
            iceberg_count=5,
            gold_total_orders=5,
        )

    def test_iceberg_count_mismatch_fails(
        self
    ):
        with self.assertRaises(
            RuntimeError
        ):
            validate_pipeline_counts(
                silver_count=5,
                iceberg_count=4,
                gold_total_orders=5,
            )

    def test_gold_count_mismatch_fails(
        self
    ):
        with self.assertRaises(
            RuntimeError
        ):
            validate_pipeline_counts(
                silver_count=5,
                iceberg_count=5,
                gold_total_orders=4,
            )


if __name__ == "__main__":
    unittest.main()
