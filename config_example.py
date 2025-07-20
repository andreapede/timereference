"""
Configuration example for Excel Timestamp Synchronizer
"""

from excel_timestamp_sync import ExcelTimestampSynchronizer

def run_synchronization():
    """
    Example configuration and execution of the synchronizer.
    Modify the paths and column names according to your Excel files.
    """
    
    # Step 1: Configure the reference file
    reference_file = "reference_data.xlsx"  # Your main reference file
    reference_timestamp_column = "timestamp"  # Column name with timestamps in reference file
    
    # Step 2: Configure files to synchronize
    file_configs = [
        {
            'file_path': 'data_file_1.xlsx',
            'timestamp_column': 'time',  # Name of timestamp column in this file
            'data_columns': ['temperature', 'humidity'],  # Columns to extract
            'sheet_name': 0  # Sheet index (0 for first sheet) or sheet name
        },
        {
            'file_path': 'data_file_2.xlsx',
            'timestamp_column': 'datetime',
            'data_columns': ['pressure', 'wind_speed', 'rainfall'],
            'sheet_name': 'Sheet1'  # You can use sheet name instead of index
        },
        {
            'file_path': 'data_file_3.xlsx',
            'timestamp_column': 'timestamp',
            'data_columns': ['voltage', 'current'],
            'sheet_name': 0
        }
    ]
    
    # Step 3: Initialize synchronizer
    synchronizer = ExcelTimestampSynchronizer(
        reference_file=reference_file,
        reference_timestamp_column=reference_timestamp_column,
        tolerance_seconds=5,  # Allow 5 seconds tolerance for timestamp matching
        duplicate_strategy='mean'  # How to handle duplicate timestamps
        # Options: 'mean', 'first', 'last', 'max', 'min'
    )
    
    # Step 4: Run synchronization
    output_file = "synchronized_output.xlsx"
    success = synchronizer.synchronize_files(file_configs, output_file)
    
    if success:
        print(f"✅ Synchronization completed successfully!")
        print(f"📁 Output saved to: {output_file}")
        
        # Display summary
        summary = synchronizer.get_summary()
        print(f"\n📊 Summary:")
        print(f"   • Total rows: {summary['total_rows']}")
        print(f"   • Total columns: {summary['total_columns']}")
        print(f"   • Time range: {summary['timestamp_range']['start']} to {summary['timestamp_range']['end']}")
        
        print(f"\n📈 Data completeness:")
        for col, stats in summary['missing_data_summary'].items():
            completeness = 100 - stats['missing_percentage']
            print(f"   • {col}: {completeness:.1f}% complete ({stats['missing_count']} missing values)")
        
        # Show duplicate handling information
        if summary['duplicate_warnings_count'] > 0:
            print(f"\n⚠️  Duplicate timestamps handled:")
            print(f"   • Strategy used: {summary['duplicate_strategy_used']}")
            print(f"   • Total duplicates found: {summary['duplicate_warnings_count']}")
            if summary['duplicate_warnings']:
                print(f"   • Sample warnings:")
                for warning in summary['duplicate_warnings'][:3]:
                    print(f"     - {warning}")
        else:
            print(f"\n✅ No duplicate timestamps found")
    
    else:
        print("❌ Synchronization failed. Check the error messages above.")

if __name__ == "__main__":
    run_synchronization()