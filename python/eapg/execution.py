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
from collections import OrderedDict

import pyspark.sql.functions as spark_funcs
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

def _transpose_results(
        *cols: "typing.Iterable[str]"
    )-> "typing.Iterable[tuple]":
    """
        Convert a number of arrays of same length to an array of tuples with
        the same length
    """
    n_lines = len(cols[0])

    output = list()
    for i in range(n_lines):
        sub_output = list()
        for values in cols:
            if len(values) > n_lines:
                raise IndexError('No value can be longer than first cols input')
            sub_output.append(values[i])
        output.append(tuple(sub_output))

    return output


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

    assert "claims" in input_dataframes, "'claims' must be in input_dataframes"
    assert "base_table" in input_dataframes, "'base_table' must be in input dataframes"

    outputs = dict()
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
        path_output / 'eapgs_claim_level.parquet',
        )
    outputs['eapgs_claim_level'] = df_eapg_output

    LOGGER.info('Transposing EAPG results out to claim line level')
    map_columns_to_keep = OrderedDict([
        ('medicalrecordnumber', 'sequencenumber'),
        ('itemfinaleapg', 'finaleapg'),
        ('itemfinaleapgtype', 'finaleapgtype'),
        ('itemfinaleapgcategory', 'finaleapgcategory'),
        ])

    # Use a UDF to transpose because the number of lines in each claim is variable
    transposer = spark_funcs.udf(
        _transpose_results,
        returnType=spark_types.ArrayType(
            spark_types.StructType([
                spark_types.StructField(
                    final_name,
                    spark_types.StringType()
                    )
                for final_name in map_columns_to_keep.values()
                ])
            )
        )

    df_eapgs_arrays = df_eapg_output.select(
        '*',
        *[
            spark_funcs.split(spark_funcs.col(column), ';').alias('array_' + column)
            for column in map_columns_to_keep.keys()
            ]
    )

    df_eapgs_transpose = df_eapgs_arrays.select(
        spark_funcs.explode(
            transposer(*[
                spark_funcs.col('array_' + colname)
                for colname in map_columns_to_keep.keys()
            ])
        ).alias('single_row_struct')
    ).select(
        [
            spark_funcs.col('single_row_struct')[final_name].alias(final_name)
            for final_name in map_columns_to_keep.values()
            ]
    )

    LOGGER.info('Joining EAPG results with base table')
    df_base_w_eapgs = input_dataframes['base_table'].join(
        df_eapgs_transpose,
        'sequencenumber',
        how='left_outer',
        )
    df_base_w_eapgs.validate.assert_no_nulls(
        df_base_w_eapgs.columns,
        )
    sparkapp.save_df(
        df_base_w_eapgs,
        path_output / 'eapgs_claim_line_level.parquet',
        )
    outputs['eapgs_claim_line_level'] = df_base_w_eapgs

    if cleanup_claim_copies:
        shutil.rmtree(str(path_workspace))

    return outputs


if __name__ == 'main':
    pass
