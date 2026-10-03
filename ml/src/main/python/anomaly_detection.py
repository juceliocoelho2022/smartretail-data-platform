import math
from statistics import median


def calculate_residual(actual: float, predicted: float) -> float:
    return actual - predicted


def calculate_median(values: list[float]) -> float:
    return float(median(values))


def calculate_mad(values: list[float]) -> float:
    center = calculate_median(values)
    deviations = [abs(value - center) for value in values]

    return calculate_median(deviations)


def calculate_modified_z_score(
        residual: float,
        median_residual: float,
        mad: float
) -> float:
    if mad == 0.0:
        if residual == median_residual:
            return 0.0

        difference = residual - median_residual
        return math.copysign(math.inf, difference)

    return 0.6745 * (residual - median_residual) / mad


def is_anomaly(score: float, threshold: float = 3.5) -> bool:
    return abs(score) > threshold


def detect_anomaly(
        actual: float,
        predicted: float,
        historical_residuals: list[float],
        threshold: float = 3.5,
) -> dict:
    residual = calculate_residual(actual, predicted)
    median_residual = calculate_median(historical_residuals)
    mad = calculate_mad(historical_residuals)

    modified_z_score = calculate_modified_z_score(
        residual=residual,
        median_residual=median_residual,
        mad=mad,
    )

    return {
        "actual": actual,
        "predicted": predicted,
        "residual": residual,
        "median_residual": median_residual,
        "mad": mad,
        "modified_z_score": modified_z_score,
        "is_anomaly": is_anomaly(
            score=modified_z_score,
            threshold=threshold,
        ),
    }


def detect_anomalies_by_product(
        records: list[dict],
        threshold: float = 3.5,
) -> list[dict]:
    results = []

    for record in records:
        result = detect_anomaly(
            actual=record["actual"],
            predicted=record["predicted"],
            historical_residuals=record["historical_residuals"],
            threshold=threshold,
        )

        results.append(
            {
                "product_id": record["product_id"],
                **result,
            }
        )

    return results