#!/usr/bin/env python3
"""
File Data Storage Application
Reads XYZ trajectory files, stores coordinates in SQLite database,
then processes data from database to compute means and fit distributions
"""

import os
import sqlite3
import time
import sys
from pathlib import Path
from datetime import datetime

class FileDataStorage:
    def __init__(self, db_path="./data/storage.db", data_dir="./data", output_dir="./data/output"):
        """Initialize the storage application"""
        self.db_path = db_path
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.log_file = os.path.join(self.data_dir, "app.log")
        self.start_time = None
        self.timings = {}

        # Create data and output directories if they don't exist
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Initialize database
        self.init_database()
        self.log("Application initialized")

    def log(self, message):
        """Log messages to both console and file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(log_msg)

        with open(self.log_file, "a") as f:
            f.write(log_msg + "\n")

    def init_database(self):
        """Initialize SQLite database with tables for XYZ data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Table to store file metadata
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS xyz_files (
                    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    num_atoms INTEGER NOT NULL,
                    num_frames INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Table to store coordinate data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS coordinates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    frame INTEGER NOT NULL,
                    atom_index INTEGER NOT NULL,
                    x REAL NOT NULL,
                    y REAL NOT NULL,
                    z REAL NOT NULL,
                    FOREIGN KEY (file_id) REFERENCES xyz_files(file_id)
                )
            ''')

            # Create index for faster queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_file_frame
                ON coordinates(file_id, frame)
            ''')

            conn.commit()
            conn.close()
            self.log(f"Database initialized at {self.db_path}")
        except Exception as e:
            self.log(f"Error initializing database: {str(e)}")

    def store_xyz_file(self, file_path):
        """Read XYZ file and store coordinates in database"""
        store_start = time.time()
        try:
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                self.log(f"Error: File not found at {file_path}")
                return None

            self.log(f"Reading XYZ file: {file_path_obj.name}")

            read_start = time.time()
            with open(file_path, 'r') as file:
                lines = file.readlines()
            read_time = time.time() - read_start
            self.log(f"File read time: {read_time:.2f} seconds")

            num_atoms = int(lines[0].strip())
            num_frames = len(lines) // (num_atoms + 2)

            self.log(f"File contains {num_atoms} atoms and {num_frames} frames")

            db_start = time.time()
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Insert file metadata
            cursor.execute('''
                INSERT INTO xyz_files (filename, num_atoms, num_frames)
                VALUES (?, ?, ?)
            ''', (file_path_obj.name, num_atoms, num_frames))

            file_id = cursor.lastrowid

            # Insert coordinates in batches for efficiency
            batch_size = 1000
            coordinates = []

            for frame in range(num_frames):
                start_index = frame * (num_atoms + 2) + 2
                for atom_idx in range(num_atoms):
                    parts = lines[start_index + atom_idx].split()
                    x = float(parts[1])
                    y = float(parts[2])
                    z = float(parts[3])
                    coordinates.append((file_id, frame, atom_idx, x, y, z))

                    # Insert in batches
                    if len(coordinates) >= batch_size:
                        cursor.executemany('''
                            INSERT INTO coordinates (file_id, frame, atom_index, x, y, z)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', coordinates)
                        coordinates = []

            # Insert remaining coordinates
            if coordinates:
                cursor.executemany('''
                    INSERT INTO coordinates (file_id, frame, atom_index, x, y, z)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', coordinates)

            conn.commit()
            conn.close()
            db_time = time.time() - db_start
            self.log(f"Database write time: {db_time:.2f} seconds")

            total_coords = num_frames * num_atoms
            store_time = time.time() - store_start
            self.log(f"Stored {total_coords} coordinate records for file_id {file_id}")
            self.log(f"Total store time: {store_time:.2f} seconds")
            return file_id

        except Exception as e:
            self.log(f"Error storing XYZ file: {str(e)}")
            return None

    def get_coordinates_from_db(self, file_id):
        """Retrieve coordinates from database for a given file_id"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get file metadata
            cursor.execute('SELECT num_atoms, num_frames FROM xyz_files WHERE file_id = ?', (file_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return None, None, None

            num_atoms, num_frames = result

            # Get all coordinates ordered by frame and atom_index
            cursor.execute('''
                SELECT frame, atom_index, x, y, z
                FROM coordinates
                WHERE file_id = ?
                ORDER BY frame, atom_index
            ''', (file_id,))

            rows = cursor.fetchall()
            conn.close()

            # Reshape into numpy arrays
            import numpy as np
            x_coords = np.zeros((num_frames, num_atoms))
            y_coords = np.zeros((num_frames, num_atoms))
            z_coords = np.zeros((num_frames, num_atoms))

            for frame, atom_idx, x, y, z in rows:
                x_coords[frame, atom_idx] = x
                y_coords[frame, atom_idx] = y
                z_coords[frame, atom_idx] = z

            self.log(f"Retrieved coordinates for file_id {file_id}: {num_frames} frames, {num_atoms} atoms")
            return x_coords, y_coords, z_coords

        except Exception as e:
            self.log(f"Error retrieving coordinates: {str(e)}")
            return None, None, None

    def get_file_ids(self):
        """Get all file IDs from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT file_id, filename FROM xyz_files ORDER BY created_at DESC')
            files = cursor.fetchall()
            conn.close()
            return files
        except Exception as e:
            self.log(f"Error retrieving file IDs: {str(e)}")
            return []

    def detect_volume_type(self):
        """Detect the type of volume being used"""
        try:
            # Check if running in Docker
            if os.path.exists('/.dockerenv'):
                # Check if /data is a tmpfs mount
                with open('/proc/mounts', 'r') as f:
                    mounts = f.read()
                    if 'tmpfs' in mounts and '/data' in mounts:
                        return "TMPFS (In-Memory)"

                # Check if /data is a bind mount (check if it's a symlink or different device)
                # For named volumes, it's typically a Docker-managed volume
                # For bind mounts, it's the host filesystem
                stat_data = os.stat('/data')
                stat_input = os.stat('/data/input') if os.path.exists('/data/input') else None

                # If input directory exists and is accessible, likely bind mount
                if os.path.exists('/data/input') and os.access('/data/input', os.R_OK):
                    # Try to detect if it's a bind mount by checking if parent is accessible
                    try:
                        parent_stat = os.stat(os.path.dirname(os.path.abspath('/data/input')))
                        return "BIND VOLUME (Host Filesystem)"
                    except:
                        return "NAMED VOLUME (Docker Managed)"

                return "NAMED VOLUME (Docker Managed)"
            else:
                return "LOCAL FILESYSTEM (Not Docker)"
        except Exception as e:
            return f"UNKNOWN (Error: {str(e)})"

    def _process_dataset(self, dataset_name, xyz_files):
        """Run the store + process pipeline for one dataset (subfolder or flat).

        Returns (step1_time, step2_time) in seconds, or (0, 0) on total failure.
        Output files are written to a subdirectory of self.output_dir named after
        the dataset (e.g. data/output/dataset_1/). For the flat-fallback case
        dataset_name is an empty string and self.output_dir is used as-is.
        """
        step1_time = step2_time = 0.0

        # Step 1: Store all XYZ files in database
        self.log("\n" + "="*70)
        self.log("STEP 1: Storing XYZ files in database")
        self.log("="*70)
        step1_start = time.time()

        file_ids = []
        for xyz_file in xyz_files:
            file_id = self.store_xyz_file(str(xyz_file))
            if file_id:
                file_ids.append(file_id)

        step1_time = time.time() - step1_start
        self.log(f"\nStep 1 Total Time: {step1_time:.2f} seconds")

        if not file_ids:
            self.log("No files were successfully stored in database")
            return step1_time, step2_time

        # Step 2: Process data from database
        self.log("\n" + "="*70)
        self.log("STEP 2: Processing data from database")
        self.log("="*70)
        step2_start = time.time()

        # Preflight: scientific Python stack on macOS requires a supported Python.
        # Python 3.8 is EOL and numpy wheels for it can crash on newer macOS versions.
        if sys.platform == "darwin" and sys.version_info < (3, 10):
            self.log(
                "Local processing requires Python 3.10+ on macOS. "
                f"Detected Python {sys.version.split()[0]}. "
                "Use Docker (recommended) or install Python 3.11+ and recreate the venv."
            )
            return step1_time, step2_time

        # Route outputs to a dataset-specific subdirectory so files from
        # different datasets never collide (e.g. data/output/dataset_1/).
        # For the flat-fallback case (empty name) keep the original output_dir.
        original_output_dir = self.output_dir
        if dataset_name:
            dataset_output_dir = os.path.join(self.output_dir, dataset_name)
            Path(dataset_output_dir).mkdir(parents=True, exist_ok=True)
            self.output_dir = dataset_output_dir

        try:
            from XYZProcessor import process_from_db
            for file_id in file_ids:
                self.log(f"\nProcessing file_id: {file_id}")
                process_from_db(self, file_id)
        finally:
            self.output_dir = original_output_dir

        step2_time = time.time() - step2_start
        self.log(f"\nStep 2 Total Time: {step2_time:.2f} seconds")

        return step1_time, step2_time

    def process_data_files(self):
        """Store XYZ files in database, then process them.

        Scans DATA_DIR/input/ for subfolders; each subfolder is treated as an
        independent dataset and processed in isolation. If no subfolders exist,
        falls back to scanning DATA_DIR/input/ directly for .xyz files.
        """
        process_start = time.time()
        try:
            input_dir = os.path.join(self.data_dir, "input")
            Path(input_dir).mkdir(parents=True, exist_ok=True)

            # Detect volume type once for the whole run
            volume_type = self.detect_volume_type()
            self.log("\n" + "="*70)
            self.log(f"VOLUME TYPE DETECTED: {volume_type}")
            self.log("="*70)

            # Build list of (dataset_name, [xyz_files]) to process
            subfolders = sorted([d for d in Path(input_dir).iterdir() if d.is_dir()])

            if subfolders:
                datasets = [(sf.name, list(sf.glob("*.xyz"))) for sf in subfolders]
            else:
                self.log("WARNING: No subfolders found in input dir, falling back to flat file scan")
                flat_files = list(Path(input_dir).glob("*.xyz"))
                if not flat_files:
                    self.log(f"No XYZ files found in {input_dir}")
                    return
                datasets = [("", flat_files)]

            total_step1 = total_step2 = 0.0

            for dataset_name, xyz_files in datasets:
                # Skip empty subfolders
                if dataset_name and not xyz_files:
                    self.log(f"WARNING: Skipping subfolder '{dataset_name}': no .xyz files found")
                    continue

                label = f"dataset '{dataset_name}'" if dataset_name else "flat input"
                self.log("\n" + "="*70)
                self.log(f"PROCESSING {label.upper()}")
                self.log("="*70)

                try:
                    s1, s2 = self._process_dataset(dataset_name, xyz_files)
                    total_step1 += s1
                    total_step2 += s2
                except Exception as e:
                    self.log(f"Error processing dataset '{dataset_name}': {str(e)}")
                    import traceback
                    self.log(traceback.format_exc())
                    continue

            # Total execution time summary
            total_time = time.time() - process_start
            self.log("\n" + "="*70)
            self.log("EXECUTION TIME SUMMARY")
            self.log("="*70)
            self.log(f"Volume Type: {volume_type}")
            self.log(f"Step 1 (Store to DB): {total_step1:.2f} seconds")
            self.log(f"Step 2 (Process from DB): {total_step2:.2f} seconds")
            self.log(f"Total Execution Time: {total_time:.2f} seconds")
            self.log("="*70 + "\n")

            self.timings = {
                'volume_type': volume_type,
                'step1_time': total_step1,
                'step2_time': total_step2,
                'total_time': total_time,
            }

        except Exception as e:
            self.log(f"Error processing data files: {str(e)}")
            import traceback
            self.log(traceback.format_exc())


def main():
    """Main application entry point"""
    # Benchmark mode: delegate entirely to BenchmarkRunner
    if os.environ.get("BENCHMARK_MODE", "0") == "1":
        config_path = os.environ.get("BENCHMARK_CONFIG", "benchmark_config.yaml")
        sys.path.insert(0, os.path.dirname(__file__))
        from benchmark_runner import BenchmarkRunner
        runner = BenchmarkRunner(config_path=config_path)
        runner.run()
        return

    main_start = time.time()
    db_path = os.environ.get("DB_PATH", "./data/storage.db")
    data_dir = os.environ.get("DATA_DIR", "./data")
    output_dir = os.environ.get("OUTPUT_DIR", "./data/output")
    app = FileDataStorage(db_path=db_path, data_dir=data_dir, output_dir=output_dir)
    app.start_time = main_start

    print("\n" + "="*70)
    print("FILE DATA STORAGE APPLICATION")
    print("="*70 + "\n")

    # Process XYZ files from input directory
    app.process_data_files()

    # Final summary
    total_main_time = time.time() - main_start
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)
    print(f"Total Application Runtime: {total_main_time:.2f} seconds")
    print(f"Total Application Runtime: {total_main_time/60:.2f} minutes")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()