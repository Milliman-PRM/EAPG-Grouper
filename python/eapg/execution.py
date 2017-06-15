"""
### CODE OWNERS: Ben Copeland, Chas Busenburg

### OBJECTIVE:
  Functions for executing the EAPG software

### DEVELOPER NOTES:
  <none>
"""
import logging
import typing
import subprocess
import os
import shutil
from pathlib import Path
from functools import partial

import eapg.shared
from prm.spark.io_txt import encode_rows_to_strings, build_structtype_from_csv
from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)

EAPG_VERSION = '17039'
PATH_EAPG_GROUPER = Path(r'C:\Program Files\3mhis\v2017.1.2\cgs\cgs_console.exe')

PATH_TEMPLATES = Path(os.environ['eapg_grouper_home']) / 'templates'
PATH_INPUT_TEMPLATE = PATH_TEMPLATES / 'prm_eapgs_in.2017.1.2.dic'
PATH_OUTPUT_TEMPLATE = PATH_TEMPLATES / 'prm_eapgs_out.2017.1.2.dic'

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _public_log_check(
        id_partition: int,
        path_logs_public: typing.Optional[Path],
        options: typing.Dict
    ) -> dict:
    """ check if path_logs_public exists, """
    print("Checking Public_Log_Path_{}".format(id_partition))
    if path_logs_public:
        options['error_log'] = path_logs_public / 'error_log_{}.txt'.format(id_partition)
        options['edit_log'] = path_logs_public / 'edit_log_{}.txt'.format(id_partition)
    return options

def _compose_options(
        id_partition: int,
        path_input_file: Path,
        path_output_file: Path,
        path_logs_public: typing.Optional[Path],
        **kwargs_eapg
    ) -> dict:
    print("Composing options dictionary for partition_{}".format(id_partition))

    initial_options = {
        'input': path_input_file.as_posix(),
        'input_template': PATH_INPUT_TEMPLATE.as_posix(),
        'upload': path_output_file.as_posix(),
        'upload_template': PATH_OUTPUT_TEMPLATE.as_posix(),
        'input_header': 'off',
        'schedule': 'off',
        'grouper': EAPG_VERSION,
        'input_date_format': 'yyyy-MM-dd',
        }
    pub_log_options = _public_log_check(
        id_partition,
        path_logs_public,
        initial_options
    )
    pub_log_options.update(**kwargs_eapg)

    return pub_log_options

def _subprocess_array_create(id_partition: int, options: dict) -> list:
    print("Creating subprocess array for partition {}".format(id_partition))
    args = [str(PATH_EAPG_GROUPER)]
    for key, val in options.items():
        args.append('-' + key)
        args.append(str(val))

    return args

def _subprocess_partition(id_partition: int, options: dict) -> str:
    args = _subprocess_array_create(id_partition, options)

    print("Starting subprocess for partition {}".format(id_partition))

    subprocess.run(
        args,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        check=True,
        )
    print("Finish subprocess for partition {}".format(id_partition))




def _run_eapg_grouper_on_partition(# pylint: disable=too-many-locals
        id_partition: int,
        iter_claims: "typing.Iterable[str]",
        *,
        path_workspace: Path,
        path_logs_public: typing.Optional[Path]=None,
        cleanup_claim_copies: bool=True,
        **kwargs_eapg
    ) -> None:  # pragma: no cover
    """Execute the EAPG software on a single partition"""
    print("Starting EAPG_Grouper on partition {}".format(id_partition))

    path_eapg_io = Path.cwd() / "eapg_grouper_io" / "part_{}".format(id_partition)
    path_eapg_io.mkdir(
        parents=True,
        exist_ok=True,
        )

    path_input_file = path_eapg_io / 'eapg_in.csv'
    path_output_file = path_workspace / 'eapgs_out_{}.csv'.format(id_partition)

    options = _compose_options(
        id_partition,
        path_input_file,
        path_output_file,
        path_logs_public,
        **kwargs_eapg
    )
    with path_input_file.open('w') as fh_input:
        for claim in iter_claims:
            fh_input.write(claim + "\n")

    _subprocess_partition(id_partition, options)

    yield path_output_file.name

    if cleanup_claim_copies:
        shutil.rmtree(str(path_eapg_io)) # Cleanup extra claim copies lying around


def run_eapg_grouper(
        sparkapp: SparkApp,
        input_dataframes: "typing.Mapping[str, pyspark.sql.DataFrame]",
        path_output: Path,
        *,
        path_network_io: typing.Optional[Path]=None,
        path_logs_public: typing.Optional[Path]=None,
        cleanup_claim_copies: bool=True,
        output_struct: typing.Optional[spark_types.StructType]=None,
        **kwargs_eapg
    ) -> "typing.Mapping[str, pyspark.sql.DataFrame]":
    """Execute the EAPG software"""

    if path_network_io:
        path_workspace = path_network_io
    else:
        path_workspace = path_output / '_temp_eapg_grouper'

    path_workspace.mkdir(
        exist_ok=True,
        )

    rdd_claims = input_dataframes['claims'].rdd.mapPartitions(
        partial(
            encode_rows_to_strings,
            schema=input_dataframes['claims'].schema,
            delimiter_csv=',',
            )
        )
    rdd_results = rdd_claims.mapPartitionsWithIndex(
        partial(
            _run_eapg_grouper_on_partition,
            path_workspace=path_workspace,
            path_logs_public=path_logs_public,
            cleanup_claim_copies=cleanup_claim_copies,
            **kwargs_eapg
            )
        )
    rdd_results.count() # Force a realization
    if not output_struct:
        output_struct = build_structtype_from_csv(
            eapg.shared.PATH_SCHEMAS / 'eapgs_out.csv'
            )

    partitions_output = [
        str(_path)
        for _path in path_workspace.glob('eapgs_out_*.csv')
        ]
    df_eapg_output = sparkapp.session.read.csv(
        partitions_output,
        schema=output_struct,
        header=False,
        )
    sparkapp.save_df(
        df_eapg_output,
        path_output / 'eapgs_out.parquet',
        )

    if cleanup_claim_copies:
        shutil.rmtree(str(path_workspace))

    return df_eapg_output




if __name__ == 'main':
    pass
