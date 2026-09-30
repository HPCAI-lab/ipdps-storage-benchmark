import os
import time
import numpy as np
import pandas as pd
import matplotlib

# Avoid interactive UI backends in headless environments (common in Docker/CI/SSH).
if not os.environ.get("DISPLAY"):
    matplotlib.use("Agg")

import matplotlib.pyplot as plt
from scipy import stats
from scipy.stats import distributions
from fitter import Fitter, get_common_distributions

class XYZProcessor:
    def __init__(self, x_coords=None, y_coords=None, z_coords=None):
        """Initialize with coordinate arrays (from database)"""
        self.x_coords = x_coords
        self.y_coords = y_coords
        self.z_coords = z_coords

    def load_from_db(self, storage_app, file_id):
        """Load coordinates from database"""
        x_coords, y_coords, z_coords = storage_app.get_coordinates_from_db(file_id)
        if x_coords is not None:
            self.x_coords = x_coords
            self.y_coords = y_coords
            self.z_coords = z_coords
            return True
        return False

    def save_coordinates(self, x_file, y_file, z_file):
        np.savetxt(x_file, self.x_coords, delimiter=",")
        np.savetxt(y_file, self.y_coords, delimiter=",")
        np.savetxt(z_file, self.z_coords, delimiter=",")

    def compute_means(self):
        mean_x = np.mean(self.x_coords, axis=1)
        mean_y = np.mean(self.y_coords, axis=1)
        mean_z = np.mean(self.z_coords, axis=1)

        mean_df = pd.DataFrame({
            'mean_x': mean_x,
            'mean_y': mean_y,
            'mean_z': mean_z
        })

        return mean_df

#finding out dynamic fitter distribution
class DistributionFitter:
    def __init__(self, data):
        self.data = data

    def fit_distribution(self):
        f = Fitter(self.data, distributions=get_common_distributions())
        f.fit()
        best_distribution = f.get_best(method='sumsquare_error')
        return best_distribution

    def summarize(self):
        f = Fitter(self.data, distributions=get_common_distributions())
        f.fit()
        f.summary()


#refrence generation for KCUSUM
def generate_random_samples(dist_name, params, size):

    dist = getattr(distributions, dist_name)
    loc = params.get('loc', 0)
    scale = params.get('scale', 1)
    args = [params[key] for key in params if key not in ['loc', 'scale']]
    return dist.rvs(*args, loc=loc, scale=scale, size=size)


#core KCUSUM with all methods and class level implementation

class KCUSUMDetector:
    def __init__(self, threshold, delta):
        self.h = threshold
        self.delta = delta

    @staticmethod
    def gk(x, y):
        return np.exp(-((x - y) ** 2) / 2.0)

    def mmd(self, x_n_1, x_n, y_n_1, y_n):
        xx = self.gk(x_n_1, x_n)
        yy = self.gk(y_n_1, y_n)
        xy = self.gk(x_n_1, y_n)
        yx = self.gk(x_n, y_n_1)
        return xx + yy - xy - yx

    def run(self, x_1, x_2, yz_1, yz_2):
        T_kcusum = 0
        Z_n = 0
        stat_values = []
        index_1 = []

        for i in np.arange(2, len(x_1), 2):
            av_n = self.mmd(x_1[i], x_2[i - 1], yz_1[i], yz_2[i - 1])
            c_n = av_n - self.delta
            Z_n += c_n
            stat_values.append(Z_n)

            if Z_n < 0:
                Z_n = 0

            if Z_n > self.h:
                T_kcusum = i
                break

        index_1 = np.arange(2, 2 * len(stat_values) + 1, 2)
        plt.plot(index_1, stat_values)
        plt.axhline(self.h, color='r', linestyle='-')
        plt.ylabel('Threshold Value (Z_n)')
        plt.xlabel('Time')
        plt.xticks(fontsize=8)
        plt.scatter(index_1[-1], self.h)
        plt.text(index_1[-1], self.h, 'CP Detected', horizontalalignment='right')
        # Showing plots can crash/hang in non-interactive environments; close instead.
        if os.environ.get("DISPLAY"):
            plt.show()
        else:
            plt.close()

        return T_kcusum


# Main function to process from database
def process_from_db(storage_app, file_id):
    """Process XYZ data from database"""
    process_start = time.time()

    # Initialize processor and load from database
    load_start = time.time()
    xyz_processor = XYZProcessor()
    if not xyz_processor.load_from_db(storage_app, file_id):
        storage_app.log(f"Failed to load coordinates for file_id {file_id}")
        return
    load_time = time.time() - load_start
    storage_app.log(f"Database load time: {load_time:.2f} seconds")

    # Save coordinates to CSV files in output directory
    save_start = time.time()
    output_dir = storage_app.output_dir
    x_file = os.path.join(output_dir, f"x_coords_file_{file_id}.csv")
    y_file = os.path.join(output_dir, f"y_coords_file_{file_id}.csv")
    z_file = os.path.join(output_dir, f"z_coords_file_{file_id}.csv")

    xyz_processor.save_coordinates(x_file, y_file, z_file)
    save_time = time.time() - save_start
    storage_app.log(f"Saved coordinate files: {x_file}, {y_file}, {z_file}")
    storage_app.log(f"Coordinate save time: {save_time:.2f} seconds")

    # Compute mean values
    mean_start = time.time()
    mean_df = xyz_processor.compute_means()
    mean_file = os.path.join(output_dir, f"mean_values_file_{file_id}.csv")
    mean_df.to_csv(mean_file, index=False)
    mean_time = time.time() - mean_start
    storage_app.log(f"Saved mean values to {mean_file}")
    storage_app.log(f"Mean computation time: {mean_time:.2f} seconds")

    # Fit distributions
    fit_start = time.time()
    storage_app.log("Fitting distributions for mean_x...")
    dist_fitter_x = DistributionFitter(mean_df['mean_x'])
    best_distribution_x = dist_fitter_x.fit_distribution()

    storage_app.log("Fitting distributions for mean_y...")
    dist_fitter_y = DistributionFitter(mean_df['mean_y'])
    best_distribution_y = dist_fitter_y.fit_distribution()

    storage_app.log("Fitting distributions for mean_z...")
    dist_fitter_z = DistributionFitter(mean_df['mean_z'])
    best_distribution_z = dist_fitter_z.fit_distribution()
    fit_time = time.time() - fit_start
    storage_app.log(f"Distribution fitting time: {fit_time:.2f} seconds")

    dist_x_name, params_x = list(best_distribution_x.items())[0]
    dist_y_name, params_y = list(best_distribution_y.items())[0]
    dist_z_name, params_z = list(best_distribution_z.items())[0]

    storage_app.log(f"Best distribution for mean_x: {dist_x_name}")
    storage_app.log(f"Best distribution for mean_y: {dist_y_name}")
    storage_app.log(f"Best distribution for mean_z: {dist_z_name}")

    ref_start = time.time()
    ref_data_x = generate_random_samples(dist_x_name, params_x, len(mean_df['mean_x']))
    ref_data_y = generate_random_samples(dist_y_name, params_y, len(mean_df['mean_y']))
    ref_data_z = generate_random_samples(dist_z_name, params_z, len(mean_df['mean_z']))
    ref_time = time.time() - ref_start
    storage_app.log(f"Reference data generation time: {ref_time:.2f} seconds")

    # KCUSUM detection using the dynamically generated reference data
    kcusum_start = time.time()
    storage_app.log("Running KCUSUM detection...")
    detector = KCUSUMDetector(threshold=0.015, delta=0.1)
    change_point = detector.run(mean_df['mean_z'], mean_df['mean_z'], ref_data_z, ref_data_z)
    kcusum_time = time.time() - kcusum_start
    storage_app.log(f"KCUSUM detection time: {kcusum_time:.2f} seconds")

    if change_point > 0:
        storage_app.log(f"Change point detected at index: {change_point}")
    else:
        storage_app.log("No change point detected")

    total_process_time = time.time() - process_start
    storage_app.log(f"\nProcessing breakdown for file_id {file_id}:")
    storage_app.log(f"  - Database load: {load_time:.2f}s")
    storage_app.log(f"  - Save coordinates: {save_time:.2f}s")
    storage_app.log(f"  - Compute means: {mean_time:.2f}s")
    storage_app.log(f"  - Fit distributions: {fit_time:.2f}s")
    storage_app.log(f"  - Generate reference: {ref_time:.2f}s")
    storage_app.log(f"  - KCUSUM detection: {kcusum_time:.2f}s")
    storage_app.log(f"  - Total processing: {total_process_time:.2f}s")
    storage_app.log(f"Completed processing for file_id {file_id}")


#main('./file-storage-app/input_data/1h9t_traj.xyz')