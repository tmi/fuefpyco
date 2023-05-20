"""
Generators for pandas DataFrames. The canonical usecase is for random dataframes with exposed seed and functional
dependencies or correlations among columns, to facilitate ML synthetic dataset creation.
"""

import numpy as np
import pandas as pd

# TODO work in progress


def geometric_dataset(size=100_000, clusters=5, dimensions=20, seed=42):
    base_generator = np.random.default_rng(seed)
    labels = base_generator.choice(a=clusters, size=size)
    centroids = base_generator.random(size=(clusters, dimensions))
    points = pd.DataFrame(centroids[labels])
    dimension_scales = base_generator.exponential(scale=0.1, size=dimensions)
    for i in range(dimensions):
        points[i] += base_generator.normal(scale=dimension_scales[i])
    points["labels"] = labels
    return pd.DataFrame(centroids), points
