import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
import logging

class ExcelTimestampSynchronizer:
    """
    Synchronizes data from multiple Excel files based on timestamp matching.
    """
    
    def __init__(self, reference_file, reference_timestamp_column, tolerance_seconds=0, 
                 duplicate_strategy='mean'):
        """
        Initialize the synchronizer.
        
        Args:
            reference_file (str): Path to the reference Excel file
            reference_timestamp_column (str): Name of the timestamp column in reference file
            tolerance_seconds (int): Tolerance in seconds for timestamp matching (default: 0 for exact match)
            duplicate_strategy (str): How to handle duplicate timestamps:
                - 'mean': Take the average of all matching values
                - 'first': Use the first occurrence
                - 'last': Use the last occurrence
                - 'max': Use the maximum value
                - 'min': Use the minimum value
        """
        self.reference_file = reference_file
        self.reference_timestamp_column = reference_timestamp_column
        self.tolerance_seconds = tolerance_seconds
        self.duplicate_strategy = duplicate_strategy
        self.reference_data = None
        self.synchronized_data = None
        self.duplicate_warnings = []
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def load_reference_data(self):
        """Load and prepare the reference Excel file."""
        try:
            self.reference_data = pd.read_excel(self.reference_file)
            
            # Convert timestamp column to datetime
            self.reference_data[self.reference_timestamp_column] = pd.to_datetime(
                self.reference_data[self.reference_timestamp_column]
            )
            
            self.logger.info(f"Loaded reference file: {self.reference_file}")
            self.logger.info(f"Reference data shape: {self.reference_data.shape}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading reference file: {e}")
            return False
    
    def synchronize_files(self, file_configs, output_file):
        """
        Synchronize multiple Excel files with the reference timestamps.
        
        Args:
            file_configs (list): List of dictionaries with file configuration
                Each dict should contain:
                - 'file_path': path to Excel file
                - 'timestamp_column': name of timestamp column
                - 'data_columns': list of column names to extract
                - 'sheet_name': (optional) sheet name, defaults to first sheet
            output_file (str): Path for the output synchronized Excel file
        """
        if self.reference_data is None:
            if not self.load_reference_data():
                return False
        
        # Start with reference data
        result_data = self.reference_data.copy()
        
        for config in file_configs:
            try:
                self._process_file(config, result_data)
            except Exception as e:
                self.logger.error(f"Error processing file {config['file_path']}: {e}")
                continue
        
        # Save synchronized data
        try:
            result_data.to_excel(output_file, index=False)
            self.logger.info(f"Synchronized data saved to: {output_file}")
            self.synchronized_data = result_data
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving output file: {e}")
            return False
    
    def _process_file(self, config, result_data):
        """Process a single Excel file and merge with result data."""
        file_path = config['file_path']
        timestamp_col = config['timestamp_column']
        data_columns = config['data_columns']
        sheet_name = config.get('sheet_name', 0)  # Default to first sheet
        
        self.logger.info(f"Processing file: {file_path}")
        
        # Load the Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Convert timestamp column to datetime
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Select only required columns
        columns_to_keep = [timestamp_col] + data_columns
        df_filtered = df[columns_to_keep].copy()
        
        # Merge with reference timestamps
        merged_data = self._merge_by_timestamp(
            result_data, df_filtered, timestamp_col, data_columns
        )
        
        # Update result_data with merged columns
        for col in data_columns:
            if col in merged_data.columns:
                result_data[f"{Path(file_path).stem}_{col}"] = merged_data[col]
        
        self.logger.info(f"Added {len(data_columns)} columns from {file_path}")
    
    def _merge_by_timestamp(self, reference_df, data_df, timestamp_col, data_columns):
        """Merge data based on timestamp matching with tolerance."""
        reference_timestamps = reference_df[self.reference_timestamp_column]
        data_timestamps = data_df[timestamp_col]
        
        # Initialize result columns with NaN
        result_dict = {col: [np.nan] * len(reference_df) for col in data_columns}
        
        for ref_idx, ref_time in enumerate(reference_timestamps):
            # Find closest timestamp within tolerance
            time_diffs = abs(data_timestamps - ref_time).dt.total_seconds()
            
            if self.tolerance_seconds > 0:
                # Find timestamps within tolerance
                within_tolerance = time_diffs <= self.tolerance_seconds
                if within_tolerance.any():
                    matching_indices = time_diffs[within_tolerance].index
                    
                    if len(matching_indices) == 1:
                        # Single match - use it directly
                        match_idx = matching_indices[0]
                        for col in data_columns:
                            result_dict[col][ref_idx] = data_df.loc[match_idx, col]
                    else:
                        # Multiple matches - handle duplicates
                        self._handle_duplicate_timestamps(
                            data_df, matching_indices, data_columns, result_dict, ref_idx, ref_time
                        )
            else:
                # Exact match only
                exact_matches = time_diffs == 0
                if exact_matches.any():
                    matching_indices = time_diffs[exact_matches].index
                    
                    if len(matching_indices) == 1:
                        # Single match
                        match_idx = matching_indices[0]
                        for col in data_columns:
                            result_dict[col][ref_idx] = data_df.loc[match_idx, col]
                    else:
                        # Multiple exact matches - handle duplicates
                        self._handle_duplicate_timestamps(
                            data_df, matching_indices, data_columns, result_dict, ref_idx, ref_time
                        )
        
        return pd.DataFrame(result_dict)
    
    def _handle_duplicate_timestamps(self, data_df, matching_indices, data_columns, 
                                   result_dict, ref_idx, ref_time):
        """Handle multiple rows with the same timestamp."""
        duplicate_data = data_df.loc[matching_indices]
        
        # Log the duplicate occurrence
        warning_msg = f"Found {len(matching_indices)} duplicate timestamps for {ref_time}"
        self.duplicate_warnings.append(warning_msg)
        self.logger.warning(warning_msg)
        
        for col in data_columns:
            values = duplicate_data[col].dropna()  # Remove NaN values
            
            if len(values) == 0:
                # All values are NaN
                result_dict[col][ref_idx] = np.nan
            elif len(values) == 1:
                # Only one non-NaN value
                result_dict[col][ref_idx] = values.iloc[0]
            else:
                # Multiple non-NaN values - apply strategy
                if self.duplicate_strategy == 'mean':
                    # Only calculate mean for numeric data
                    if pd.api.types.is_numeric_dtype(values):
                        result_dict[col][ref_idx] = values.mean()
                    else:
                        result_dict[col][ref_idx] = values.iloc[0]  # First for non-numeric
                elif self.duplicate_strategy == 'first':
                    result_dict[col][ref_idx] = values.iloc[0]
                elif self.duplicate_strategy == 'last':
                    result_dict[col][ref_idx] = values.iloc[-1]
                elif self.duplicate_strategy == 'max':
                    if pd.api.types.is_numeric_dtype(values):
                        result_dict[col][ref_idx] = values.max()
                    else:
                        result_dict[col][ref_idx] = values.iloc[0]  # First for non-numeric
                elif self.duplicate_strategy == 'min':
                    if pd.api.types.is_numeric_dtype(values):
                        result_dict[col][ref_idx] = values.min()
                    else:
                        result_dict[col][ref_idx] = values.iloc[0]  # First for non-numeric
                else:
                    # Default to first
                    result_dict[col][ref_idx] = values.iloc[0]
    
    def get_summary(self):
        """Get a summary of the synchronization results."""
        if self.synchronized_data is None:
            return "No synchronized data available."
        
        summary = {
            'total_rows': len(self.synchronized_data),
            'total_columns': len(self.synchronized_data.columns),
            'timestamp_range': {
                'start': self.synchronized_data[self.reference_timestamp_column].min(),
                'end': self.synchronized_data[self.reference_timestamp_column].max()
            },
            'duplicate_strategy_used': self.duplicate_strategy,
            'duplicate_warnings_count': len(self.duplicate_warnings),
            'duplicate_warnings': self.duplicate_warnings[:10],  # Show first 10 warnings
            'missing_data_summary': {}
        }
        
        # Calculate missing data for each column
        for col in self.synchronized_data.columns:
            if col != self.reference_timestamp_column:
                missing_count = self.synchronized_data[col].isna().sum()
                summary['missing_data_summary'][col] = {
                    'missing_count': missing_count,
                    'missing_percentage': (missing_count / len(self.synchronized_data)) * 100
                }
        
        return summary


def main():
    """Example usage of the ExcelTimestampSynchronizer."""
    
    # Example configuration
    reference_file = "reference_data.xlsx"
    reference_timestamp_column = "timestamp"
    
    # Configuration for files to synchronize
    file_configs = [
        {
            'file_path': 'sensor_data_1.xlsx',
            'timestamp_column': 'time',
            'data_columns': ['temperature', 'humidity'],
            'sheet_name': 0  # First sheet
        },
        {
            'file_path': 'sensor_data_2.xlsx',
            'timestamp_column': 'datetime',
            'data_columns': ['pressure', 'wind_speed'],
            'sheet_name': 'Data'  # Named sheet
        }
    ]
    
    # Initialize synchronizer
    synchronizer = ExcelTimestampSynchronizer(
        reference_file=reference_file,
        reference_timestamp_column=reference_timestamp_column,
        tolerance_seconds=1  # Allow 1 second tolerance
    )
    
    # Perform synchronization
    output_file = "synchronized_data.xlsx"
    success = synchronizer.synchronize_files(file_configs, output_file)
    
    if success:
        # Print summary
        summary = synchronizer.get_summary()
        print("\nSynchronization Summary:")
        print(f"Total rows: {summary['total_rows']}")
        print(f"Total columns: {summary['total_columns']}")
        print(f"Timestamp range: {summary['timestamp_range']['start']} to {summary['timestamp_range']['end']}")
        
        print("\nMissing data summary:")
        for col, stats in summary['missing_data_summary'].items():
            print(f"  {col}: {stats['missing_count']} missing ({stats['missing_percentage']:.1f}%)")
    else:
        print("Synchronization failed. Check the logs for details.")


if __name__ == "__main__":
    main()