# saspolarize

Run SAS code from Python with polars DataFrame input and output.

`saspolarize` is a lightweight bridge between **Python**, **SAS**, and **polars**. It lets you:

- execute SAS code from Python
- send a `polars.DataFrame` into SAS as a temporary WORK table
- export a SAS table back into Python as a `polars.DataFrame`
- capture SAS log and output text for debugging or inspection

## Why use this?

If you're moving from SAS to Python, a full rewrite is rarely the best first step.

`saspolarize` gives you a gradual path: keep existing SAS workflows running while shifting more of your pipeline into Python over time. That makes it easier to modernize safely, validate results, and avoid rewriting everything at once.


## Installation

Install the package and its dependencies with your preferred tool.

Example with `uv`:
```bash
uv sync
```
Or with `pip`:
```bash
pip install .
```
## Requirements

- Python `>= 3.11` (earlier should work)
- A local SAS installation
- The following Python dependencies:
  - `polars`
  - `loguru`

## Configure SAS

By default, the library looks for SAS here:
```text
C:\Program Files\SASHome\SASFoundation\9.4\sas.exe
```
You can also point it to a different executable with the `SAS_EXECUTABLE` environment variable.

### Windows PowerShell
```powershell
$env:SAS_EXECUTABLE="C:\Path\To\sas.exe"
```
Or pass the executable path directly in code.

## Quick Start

### Run plain SAS code
```python
from saspolarize import run_sas_code

result = run_sas_code("""
data test;
    x = 1;
    y = 'Hello from SAS';
    output;
run;

proc print data=test;
run;
""")

print(result["success"])
print(result["return_code"])
print(result["log"])
print(result["output"])
```
## Pass a polars DataFrame into SAS

If you provide `input_df`, it is written to a temporary CSV and imported into SAS as `work.<input_table>`.
```python
import polars as pl
from saspolarize import run_sas_code

df = pl.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'score': [85.5, 92.0, 78.5],
})

result = run_sas_code(
    '''
    proc print data=work.input_df;
        title 'Data from polars';
    run;
    ''',
    input_df=df,
    input_table='input_df',
)

print(result['success'])
print(result['output'])
```
## Get a SAS table back as a polars DataFrame

If you provide `output_table`, the library exports that SAS table to CSV and reads it back into polars.

```python
import polars as pl
from saspolarize import run_sas_code

input_df = pl.DataFrame({
    'id': [1, 2, 3],
    'value': [10, 20, 30],
})

result = run_sas_code(
    '''
    data work.output_df;
        set work.input_df;
        doubled = value * 2;
    run;
    ''',
    input_df=input_df,
    input_table='input_df',
    output_table='work.output_df',
)

print(result['success'])
print(result['df'])
```
## API

### `run_sas_code(...)`

Run SAS code and optionally exchange tabular data with polars.

#### Parameters

- `sas_code: str`  
  SAS code to execute.

- `sas_exec: Path | str | None = None`  
  Path to the SAS executable. If omitted, uses `SAS_EXECUTABLE` or the default SAS 9.4 path.

- `input_df: pl.DataFrame | None = None`  
  Optional polars DataFrame to make available inside SAS.

- `input_table: str = "input_df"`  
  Name of the SAS WORK table created from `input_df`.

- `output_table: str | None = None`  
  Optional SAS table name to export and return as a polars DataFrame.

- `output_encoding: str = "utf8"`  
  Encoding used when SAS exports CSV output.

- `sas_encoding: str = "latin1"`  
  Encoding used to read SAS log and listing output.

- `infer_schema_length: int | None = None`  
  Optional value passed to polars when reading returned CSV output.

#### Returns

A dictionary with:
```python
{
    'success': bool,
    'log': str,
    'output': str,
    'return_code': int,
    'stderr': str,
    'stdout': str,
    'df': pl.DataFrame | None,
}
```
### `get_sas_exe(path=None)`

Resolve the SAS executable path from:

1. the provided `path`
2. the `SAS_EXECUTABLE` environment variable
3. the default Windows SAS 9.4 location

## How it works

`saspolarize` creates a temporary working directory for each execution and stores:

- the generated SAS program
- the SAS log file
- the SAS listing/output file
- optional CSV files used to transfer data in and out

This keeps runs isolated and avoids leaving temporary artifacts behind.

## Testing

Run tests with:
```bash
pytest
```

N.b.: tests that require SAS are skipped automatically if the configured SAS executable is not found.

## Notes

- This library assumes SAS is installed locally and callable from the host machine.
- Returned DataFrame support depends on the SAS table being exportable to CSV.
- Encoding matters: if your SAS session uses a different encoding, you may need to adjust `sas_encoding` or `output_encoding`.

## License

