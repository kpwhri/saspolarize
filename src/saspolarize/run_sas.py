"""
Run SAS code from within Python, including access to polars DataFrames (reading/returning).
"""
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from loguru import logger
import polars as pl


def get_sas_exe(path=None):
    if path is None:
        path = os.environ.get('SAS_EXECUTABLE') or r'C:\Program Files\SASHome\SASFoundation\9.4\sas.exe'
    return str(path)


def run_sas_code(
        sas_code: str,
        *,
        sas_exec: Path | str | None = None,
        input_df: pl.DataFrame | None = None,
        input_table: str = 'input_df',
        output_table: str | None = None,
        output_encoding: str = 'utf8',
        sas_encoding: str = 'latin1',
        infer_schema_length: int | None = None,
) -> dict[str, Any]:
    """Run SAS code from Python, optionally providing/receiving polars dataframe.

    Args:
        sas_code: the SAS code to execute (as a string)
        sas_exec: path to SAS executable. If None, uses env var `SAS_EXECUTABLE` or the default SAS 9.4 path
        input_df: optional Polars DataFrame to make available to SAS as `work.<input_table>`
        input_table: name for the SAS WORK table created from `input_df` (default: 'input_df')
        output_table: optional SAS table name (e.g., 'work.out') to export to CSV and read back into polars
        output_encoding: encoding used by sas when exporting csv (default: 'utf8')
        sas_encoding: encoding used by sas for lst and log files (default: 'latin1')
        infer_schema_length: optional rows to scan when inferring schema in polars `read_csv`

    Returns:
        dict: contains 'success', 'log', 'output', 'return_code', 'stderr', 'stdout', and optionally 'df'.

            ```
            # dict output will look like
            {
              'success': True/False,
              'log': log_content,  # content of SAS log file (if it exists)
              'output': output_content,  # content of lst file (if it exists)
              'return_code': returncode,  # 1/0 probably
              'stderr': stderr,
              'stdout': stdout,
              'df': pl.DataFrame,  # a polars dataframe (only if `ouput_ptable` option is supplied)
            }
            ```

    Notes:
        - when `input_df` is provided, it is written to a temp csv and imported via `proc import` into WORK.
        - when `output_table` is provided, it is exported via `proc export` to a temp csv
            and read back as a polars dataframe.
    """
    # resolve sas executable
    sas_exec = get_sas_exe(sas_exec)
    logger.info(f'Using SAS executable at: {sas_exec}')

    # create an isolated temporary working directory for all artifacts
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        logger.info(f'Creating temp directory for processing here: {tmpdir_path}')

        # if a polars DataFrame is supplied, export to CSV and build a 'prelude' to read the CSV file
        prelude = ''
        if input_df is not None:
            csv_path = tmpdir_path / f'{input_table}.csv'
            logger.info(f'Writing input polars dataframe to CSV: {csv_path}')
            # write csv with header; rely on sas guessingrows=max to infer types
            input_df.write_csv(str(csv_path))
            prelude = (
                f"filename csvfile '{str(csv_path)}';\n"
                f'proc import datafile=csvfile out=work.{input_table} dbms=csv replace;\n'
                f'  guessingrows=max;\n'
                f'  getnames=yes;\n'
                f'run;\n\n'
            )

        # if an output table is requested, append a postlude to export it to CSV for polars to read
        postlude = ''
        out_csv_path = None
        if output_table:
            out_csv_path = tmpdir_path / 'output_table.csv'
            # use filename to control encoding and path quoting in SAS
            postlude = (
                f"\nfilename outcsv '{str(out_csv_path)}' encoding='{output_encoding}';\n"
                f'proc export data={output_table} outfile=outcsv dbms=csv replace;\n'
                f'  putnames=yes;\n'
                f'run;\n'
            )

        sas_text = f'{prelude}{sas_code}{postlude}'

        # create code/log/output files inside tmp dir
        sas_path = tmpdir_path / 'code.sas'
        log_path = tmpdir_path / 'code.log'
        lst_path = tmpdir_path / 'code.lst'

        sas_path.write_text(sas_text, encoding='utf8')

        # run sas
        cmd = [sas_exec, '-sysin', str(sas_path), '-log', str(log_path), '-print', str(lst_path)]
        logger.info(f'Running command for SAS: {cmd}')
        result = subprocess.run(cmd, capture_output=True, text=True)

        # read artifacts
        log_content = ''
        output_content = ''
        if log_path.exists():
            try:
                log_content = log_path.read_text(encoding=sas_encoding, errors='replace')
            except Exception as e:
                logger.warning(f'Unable to read log file at: {log_path} due to: {e}')
        else:
            logger.info(f'Log file does not exist: {log_path}')

        if lst_path.exists():
            try:
                output_content = lst_path.read_text(encoding=sas_encoding, errors='replace')
            except Exception as e:
                logger.warning(f'Unable to read lst file at: {output_content} due to: {e}')
        else:
            logger.info(f'Lst file does not exist: {lst_path}')

        payload: dict[str, Any] = {
            'success': result.returncode == 0,
            'log': log_content,
            'output': output_content,
            'return_code': result.returncode,
            'stderr': result.stderr,
            'stdout': result.stdout,
        }

        # if an output csv was requested and exists, load it with polars
        if out_csv_path is not None and out_csv_path.exists():
            read_kwargs: dict[str, Any] = {'encoding': output_encoding}
            if infer_schema_length is not None:
                read_kwargs['infer_schema_length'] = infer_schema_length
            logger.info(f'Reading output dataset into polars: {out_csv_path}')
            payload['df'] = pl.read_csv(str(out_csv_path), **read_kwargs)
            logger.info(f'Done! This can be accessed with `result[\'df\']`.')
        else:
            payload['df'] = None if output_table else None

        return payload
