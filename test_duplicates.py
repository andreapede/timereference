"""
Test script to demonstrate duplicate timestamp handling
"""

import pandas as pd
from datetime import datetime, timedelta
from excel_timestamp_sync import ExcelTimestampSynchronizer

def create_test_files():
    """Create test Excel files with duplicate timestamps."""
    
    # Create reference file
    ref_timestamps = [
        datetime(2024, 1, 1, 10, 0, 0),
        datetime(2024, 1, 1, 10, 1, 0),
        datetime(2024, 1, 1, 10, 2, 0),
        datetime(2024, 1, 1, 10, 3, 0),
        datetime(2024, 1, 1, 10, 4, 0),
    ]
    
    ref_data = pd.DataFrame({
        'timestamp': ref_timestamps,
        'reference_id': [1, 2, 3, 4, 5]
    })
    ref_data.to_excel('test_reference.xlsx', index=False)
    print("✅ Created test_reference.xlsx")
    
    # Create data file with duplicates
    data_timestamps = [
        datetime(2024, 1, 1, 10, 0, 0),  # Duplicate
        datetime(2024, 1, 1, 10, 0, 0),  # Duplicate
        datetime(2024, 1, 1, 10, 1, 0),
        datetime(2024, 1, 1, 10, 2, 0),  # Duplicate
        datetime(2024, 1, 1, 10, 2, 0),  # Duplicate
        datetime(2024, 1, 1, 10, 2, 0),  # Duplicate
        datetime(2024, 1, 1, 10, 4, 0),
    ]
    
    sensor_data = pd.DataFrame({
        'time': data_timestamps,
        'temperature': [20.1, 20.3, 21.5, 22.0, 22.2, 21.8, 23.1],  # Different values for duplicates
        'humidity': [45, 47, 50, 52, 53, 51, 48]
    })
    sensor_data.to_excel('test_sensor_data.xlsx', index=False)
    print("✅ Created test_sensor_data.xlsx with duplicate timestamps")

def test_duplicate_strategies():
    """Test different duplicate handling strategies."""
    
    strategies = ['mean', 'first', 'last', 'max', 'min']
    
    for strategy in strategies:
        print(f"\n🔄 Testing strategy: {strategy}")
        
        synchronizer = ExcelTimestampSynchronizer(
            reference_file='test_reference.xlsx',
            reference_timestamp_column='timestamp',
            tolerance_seconds=0,  # Exact match
            duplicate_strategy=strategy
        )
        
        file_configs = [{
            'file_path': 'test_sensor_data.xlsx',
            'timestamp_column': 'time',
            'data_columns': ['temperature', 'humidity'],
            'sheet_name': 0
        }]
        
        output_file = f'test_output_{strategy}.xlsx'
        success = synchronizer.synchronize_files(file_configs, output_file)
        
        if success:
            # Read and display results
            result = pd.read_excel(output_file)
            print(f"   Results for duplicate timestamps:")
            
            # Show results for timestamps that had duplicates
            duplicate_times = [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 2, 0)
            ]
            
            for dt in duplicate_times:
                row = result[result['timestamp'] == dt]
                if not row.empty:
                    temp = row['test_sensor_data_temperature'].iloc[0]
                    humidity = row['test_sensor_data_humidity'].iloc[0]
                    print(f"   {dt}: temp={temp:.1f}, humidity={humidity}")
            
            summary = synchronizer.get_summary()
            print(f"   Duplicates found: {summary['duplicate_warnings_count']}")

if __name__ == "__main__":
    print("Creating test files with duplicate timestamps...")
    create_test_files()
    
    print("\nTesting different duplicate handling strategies...")
    test_duplicate_strategies()
    
    print("\n" + "="*50)
    print("Test completed! Check the output files to see the differences.")
    print("Original duplicate values for 10:00:00 - temp: 20.1, 20.3 | humidity: 45, 47")
    print("Original duplicate values for 10:02:00 - temp: 22.0, 22.2, 21.8 | humidity: 52, 53, 51")