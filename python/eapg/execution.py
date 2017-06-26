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
import shutil
from pathlib import Path
from functools import partial

import pyspark.sql.types as spark_types # pylint: disable=unused-import

import eapg.shared
from prm.spark.io_txt import encode_rows_to_strings, build_structtype_from_csv
from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)

EAPG_VERSION = '17039'
PATH_EAPG_GROUPER = Path(r'C:\Program Files\3mhis\v2017.1.2\cgs\cgs_console.exe')

PATH_TEMPLATES = eapg.shared.PATH_TEMPLATES
PATH_INPUT_TEMPLATE = eapg.shared.PATH_INPUT_TEMPLATE
PATH_OUTPUT_TEMPLATE = eapg.shared.PATH_OUTPUT_TEMPLATE

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_public_log_parameters(
        id_partition: int,
        path_logs_public: typing.Optional[Path]
    ) -> dict:
    """ check if path_logs_public exists, """
    print("Checking Public_Log_Path_{}".format(id_partition))
    if path_logs_public:
        public_options = {
            'error_log': path_logs_public / 'error_log_{}.txt'.format(id_partition),
            'edit_log': path_logs_public / 'edit_log_{}.txt'.format(id_partition),
        }
    else:
        public_options = dict()
    return public_options

def _compose_cli_parameters(
        id_partition: int,
        path_input_file: Path,
        path_output_file: Path,
        path_logs_public: typing.Optional[Path],
        **kwargs_eapg
    ) -> dict:
    print("Composing options dictionary for partition_{}".format(id_partition))

    initial_parameters = {
        'input': path_input_file.as_posix(),
        'input_template': PATH_INPUT_TEMPLATE.as_posix(),
        'upload': path_output_file.as_posix(),
        'upload_template': PATH_OUTPUT_TEMPLATE.as_posix(),
        'input_header': 'off',
        'schedule': 'off',
        'grouper': EAPG_VERSION,
        'input_date_format': 'yyyy-MM-dd',
        }
    pub_log_parameters = _get_public_log_parameters(
        id_partition,
        path_logs_public
    )
    cli_parameters = {**pub_log_parameters, **initial_parameters}
    cli_parameters.update(**kwargs_eapg)

    return cli_parameters

def _compose_eapg_subprocess_args(
        id_partition: int,
        options: dict,
    )-> list:
    print("Creating subprocess array for partition {}".format(id_partition))
    args = [str(PATH_EAPG_GROUPER)]
    for key, val in options.items():
        args.append('-' + key)
        args.append(str(val))

    return args

def _run_eapg_subprocess(
        id_partition: int,
        options: dict
    ) -> str:
    """Execute EAPG Grouper Command Line Subprocess"""
    args = _compose_eapg_subprocess_args(id_partition, options)

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

    options = _compose_cli_parameters(
        id_partition,
        path_input_file,
        path_output_file,
        path_logs_public,
        **kwargs_eapg
    )
    with path_input_file.open('w') as fh_input:
        for claim in iter_claims:
            fh_input.write(claim + "\n")

    _run_eapg_subprocess(id_partition, options)

    yield path_output_file.name

    if cleanup_claim_copies:
        shutil.rmtree(str(path_eapg_io)) # Cleanup extra claim copies lying around

def _assign_path_workspace(
        path_network_io: typing.Optional[Path],
        path_output: Path,
    ) -> Path:
    if path_network_io:

        LOGGER.info('workspace is set to %s',
                    str(path_network_io),
                   )
        path_workspace = path_network_io
    else:
        LOGGER.info('path_network_io was not set, thus workspace is set to %s',
                    str(path_output / 'temp_eapg_grouper'),
                   )
        path_workspace = path_output / '_temp_eapg_grouper'
    return path_workspace

def _generate_final_struct(
        output_struct: typing.Optional[spark_types.StructType],
    )-> spark_types.StructType:
    """ Generate structtypes based on whether user-gives struct"""
    if not output_struct:
        LOGGER.info("output_struct not defined, faulting to %s",
                    str(eapg.shared.PATH_SCHEMAS / 'eapgs_out.csv')
                   )
        return build_structtype_from_csv(
            eapg.shared.PATH_SCHEMAS / 'eapgs_out.csv'
        )
    return output_struct


def run_eapg_grouper(
        sparkapp: SparkApp,
        input_dataframes: "typing.Mapping[str, pyspark.sql.DataFrame]",
        path_output: Path,
        *,
        path_network_io: typing.Optional[Path]=None,
        path_logs_public: typing.Optional[Path]=None,
        cleanup_claim_copies: bool=True,
        output_struct: typing.Optional[spark_types.StructType]=None,
        add_Description: bool=True,
        **kwargs_eapg
    ) -> "typing.Mapping[str, pyspark.sql.DataFrame]":
    """Execute the EAPG software"""
    path_workspace = _assign_path_workspace(
        path_network_io,
        path_output,
    )
    path_workspace.mkdir(
        exist_ok=True,
        )
    LOGGER.info('Turning Claims Dataframe to RDD')
    rdd_claims = input_dataframes['claims'].rdd.mapPartitions(
        partial(
            encode_rows_to_strings,
            schema=input_dataframes['claims'].schema,
            delimiter_csv=',',
            )
        )
    LOGGER.info('Submitting Claims RDD Partitions to eapg grouper.')
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
    final_struct = _generate_final_struct(output_struct)
    partitions_output = [
        str(_path)
        for _path in path_workspace.glob('eapgs_out_*.csv')
        ]
    df_eapg_output = sparkapp.session.read.csv(
        partitions_output,
        schema=final_struct,
        header=False,
        )
    sparkapp.save_df(
        df_eapg_output,
        path_output / 'eapgs_out.parquet',
        )

    if cleanup_claim_copies:
        shutil.rmtree(str(path_workspace))

    return df_eapg_output


def _add_description_to_output(description_bool: bool,
                               df_input: ):
    """Adds the description of the EAPG code,category, and """

    if description_bool:
        pass
    else:
        return df_input

if __name__ == 'main':
    pass
