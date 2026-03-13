"""
Tests for the sas.py module.

These tests verify:
1. Running basic SAS code
2. Passing a polars DataFrame to SAS
3. Requesting a DataFrame from SAS output
"""
from pathlib import Path

import pytest
import polars as pl

from saspolarize import run_sas_code, get_sas_exe

SAS_EXE = Path(get_sas_exe())
SAS_NOT_FOUND = not SAS_EXE.exists()


@pytest.mark.skipif(SAS_NOT_FOUND, reason=f'SAS executable not found at {SAS_EXE}.')
def test_run_basic_sas_code():
    """Test 1: Running any SAS code.

    This test runs a simple SAS program that creates a dataset
    and prints it to verify basic SAS execution works.
    """
    sas_code = """
        data test;
            x = 1;
            y = 'Hello from SAS';
            output;
        run;

        proc print data=test;
        run;
    """

    result = run_sas_code(sas_code)

    assert result['success'], f"SAS code failed. Log:\n{result['log']}"
    assert result['return_code'] == 0
    # Check that output was generated (the proc print output)
    assert 'Hello from SAS' in result['output'] or 'Hello from SAS' in result['log']
    print("\n=== Test 1: Basic SAS Execution ===")
    print(f"Success: {result['success']}")
    print(f"Return code: {result['return_code']}")
    print(f"Output preview:\n{result['output'][:500] if result['output'] else 'No output'}")


@pytest.mark.skipif(SAS_NOT_FOUND, reason=f'SAS executable not found at {SAS_EXE}.')
def test_pass_polars_dataframe_to_sas():
    """Test 2: Passing in a small polars DataFrame.

    This test creates a polars DataFrame and passes it to SAS,
    then runs a proc print to verify the data was transferred correctly.
    """
    # Create a small polars DataFrame
    input_df = pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'score': [85.5, 92.0, 78.5, 95.0, 88.0],
    })

    sas_code = """
        proc print data=work.input_df;
            title 'Data from Polars DataFrame';
        run;

        proc means data=work.input_df n mean min max;
            var score;
        run;
    """

    result = run_sas_code(
        sas_code,
        input_df=input_df,
        input_table='input_df',
    )

    assert result['success'], f"SAS code failed. Log:\n{result['log']}"
    # Verify some names appear in the output (proc print should show them)
    assert any(name in result['output'] for name in ['Alice', 'Bob', 'Charlie']), \
        f"Expected names not found in output:\n{result['output']}"

    print("\n=== Test 2: Pass Polars DataFrame to SAS ===")
    print(f"Success: {result['success']}")
    print(f"Input DataFrame:\n{input_df}")
    print(f"SAS Output:\n{result['output']}")


@pytest.mark.skipif(SAS_NOT_FOUND, reason=f'SAS executable not found at {SAS_EXE}.')
def test_get_dataframe_from_sas_result():
    """Test 3: Requesting a DataFrame from the SAS result.

    This test passes a DataFrame to SAS, performs a transformation,
    and retrieves the result back as a polars DataFrame.
    """
    # Create input DataFrame
    input_df = pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50],
    })

    # SAS code that transforms the data
    sas_code = """
        data work.output_df;
            set work.input_df;
            /* Double the value and add a category */
            doubled = value * 2;
            if value <= 20 then category = 'Low';
            else if value <= 40 then category = 'Medium';
            else category = 'High';
        run;

        proc print data=work.output_df;
            title 'Transformed Data';
        run;
    """

    result = run_sas_code(
        sas_code,
        input_df=input_df,
        input_table='input_df',
        output_table='work.output_df',
    )

    assert result['success'], f"SAS code failed. Log:\n{result['log']}"
    assert result['df'] is not None, "Expected DataFrame in result but got None"

    # Verify the returned DataFrame has expected columns
    output_df = result['df']
    assert 'id' in output_df.columns
    assert 'value' in output_df.columns
    assert 'doubled' in output_df.columns
    assert 'category' in output_df.columns

    # Verify the transformation worked
    assert output_df['doubled'].to_list() == [20, 40, 60, 80, 100]

    print("\n=== Test 3: Get DataFrame from SAS Result ===")
    print(f"Success: {result['success']}")
    print(f"Input DataFrame:\n{input_df}")
    print(f"Output DataFrame from SAS:\n{output_df}")
    print(f"Categories: {output_df['category'].to_list()}")
