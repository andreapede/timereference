# Excel Timestamp Synchronizer

A Python program for synchronizing data from multiple Excel files based on timestamp matching using JSON configuration.

## Features

- **Reference-based synchronization**: Uses one Excel file as the main timestamp reference
- **Multiple file support**: Can process multiple Excel files with different sampling rates
- **Flexible timestamp matching**: Supports exact matching or tolerance-based matching
- **Duplicate timestamp handling**: 5 strategies for handling duplicate timestamps
- **JSON configuration**: Easy-to-edit configuration file
- **Missing data handling**: Gracefully handles missing timestamps and provides summary statistics
- **Single program**: One command to run everything

## Requirements

```bash
pip install pandas numpy openpyxl
```

## Quick Start

1. **Prepare your files**: 
   - One reference Excel file with your main timestamp column
   - Multiple data Excel files, each with their own timestamp column and data columns

2. **Create configuration**:
   Copy `config_template.json` to `config.json` and edit with your file paths:

```json
{
  "reference_file": "reference_data.xlsx",
  "reference_timestamp_column": "timestamp",
  "output_file": "synchronized_output.xlsx",
  "tolerance_seconds": 5,
  "duplicate_strategy": "mean",
  "data_files": [
    {
      "file_path": "sensor_data.xlsx",
      "timestamp_column": "time",
      "data_columns": ["temperature", "humidity"],
      "sheet_name": 0
    }
  ]
}
```

3. **Run the synchronization**:
```bash
python sync_excel.py config.json
```

Or simply (uses config.json by default):
```bash
python sync_excel.py
```

## How It Works

1. **Load Reference**: Reads the reference Excel file and its timestamp column
2. **Process Each File**: For each additional Excel file:
   - Loads the specified data columns and timestamp column
   - Matches timestamps with the reference (exact or within tolerance)
   - Adds matched data to the result
3. **Output**: Creates a single Excel file with:
   - All reference timestamps
   - Synchronized data from all files
   - Missing values where no timestamp match was found

## Configuration Options

### JSON Configuration Structure
```json
{
  "reference_file": "path/to/reference.xlsx",
  "reference_timestamp_column": "timestamp",
  "output_file": "synchronized_data.xlsx",
  "tolerance_seconds": 5,
  "duplicate_strategy": "mean",
  "data_files": [...]
}
```

### Timestamp Tolerance
- `tolerance_seconds: 0` - Exact timestamp matching only
- `tolerance_seconds: 5` - Allow 5-second difference for matching

### Duplicate Handling Strategies
- `"mean"` - Average of all matching values (default)
- `"first"` - Use the first occurrence
- `"last"` - Use the last occurrence  
- `"max"` - Use the maximum value
- `"min"` - Use the minimum value

### File Configuration
Each file in `data_files` supports:
- `file_path`: Path to the Excel file
- `timestamp_column`: Name of the timestamp column
- `data_columns`: List of column names to extract
- `sheet_name`: Sheet index (0, 1, 2...) or sheet name ("Sheet1", "Data", etc.)

## Example Output

The synchronized Excel file will contain:
- Original reference columns
- New columns named as `{filename}_{column_name}`
- Timestamps aligned across all data sources
- Summary statistics about data completeness

## Command Line Usage

```bash
# Use default config.json
python sync_excel.py

# Use custom configuration file
python sync_excel.py my_config.json

# Get help
python sync_excel.py --help
```

## Error Handling

The program handles common issues:
- Missing configuration file or invalid JSON
- Missing files or sheets
- Invalid timestamp formats
- Missing columns
- Different sampling rates
- Duplicate timestamps
- Provides detailed logging and validation