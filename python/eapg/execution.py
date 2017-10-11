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
from prm.spark.io_txt import encode_rows_to_strings, build_structtype_from_csv, import_csv
from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)

EAPG_VERSION = '17039'
PATH_EAPG_GROUPER = Path(r'C:\Program Files\3mhis\v2017.1.2\cgs\cgs_console.exe')

PATH_TEMPLATES = eapg.shared.PATH_TEMPLATES
PATH_INPUT_TEMPLATE = eapg.shared.PATH_INPUT_TEMPLATE
PATH_OUTPUT_TEMPLATE = eapg.shared.PATH_OUTPUT_TEMPLATE
N_MAX_ITEMS = 450

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_error_log_parameters(
        id_partition: int,
        path_logs_error: typing.Optional[Path]
    ) -> dict:
    """ check if path_logs_error exists, """
    print("Checking Error_Log_Path_{}".format(id_partition))
    if path_logs_error:
        error_options = {
            'error_log': path_logs_error / 'error_log_{}.txt'.format(id_partition),
        }
    else:
        error_options = dict()
    return error_options

def _compose_cli_parameters(
        id_partition: int,
        path_input_file: Path,
        path_output_file: Path,
        path_logs_error: typing.Optional[Path],
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
    error_log_parameters = _get_error_log_parameters(
        id_partition,
        path_logs_error
    )
    cli_parameters = {**error_log_parameters, **initial_parameters}
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
    args = _compose_eapg_subprocess_args(
        id_partition,
        options,
    )

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
        path_logs_error: typing.Optional[Path]=None,
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
    path_output_file = path_eapg_io / 'eapgs_out.csv'

    options = _compose_cli_parameters(
        id_partition,
        path_input_file,
        path_output_file,
        path_logs_error,
        **kwargs_eapg
    )
    with path_input_file.open('w', errors='replace') as fh_input:
        for claim in iter_claims:
            fh_input.write(claim + "\n")

    _run_eapg_subprocess(
        id_partition,
        options,
    )

    with path_output_file.open('r') as result:
        for line in result:
            yield line

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

    assert any((
        all([len(col) == len(cols[0]) for col in cols]), # All items should be same length
        all([len(col) >= N_MAX_ITEMS for col in cols]), # Unless they are over the EAPG threshold
        )), "Columns do not contain the same length"
    n_lines = len(cols[0])

    output = list()
    for i in range(n_lines):
        sub_output = list()
        for values in cols:
            try:
                sub_output.append(values[i])
            except IndexError:
                sub_output.append(None)

        output.append(tuple(sub_output))

    return output

def _convert_output_to_df(
        sparkapp: SparkApp,
        rdd: "pyspark.RDD",
        structtype: spark_types.StructType,
    ) -> "pyspark.sql.DataFrame":
    """Take the output stream and convert to dataframe"""
    LOGGER.debug("Sourcing from RDD %r", rdd)
    df_result = import_csv(
        sparkapp,
        rdd,
        structtype,
        header=False,
        delimiter=',',
        default_null_literals="",
        )
    return df_result


def run_eapg_grouper(
        sparkapp: SparkApp,
        input_dataframes: "typing.Mapping[str, pyspark.sql.DataFrame]",
        path_output: Path,
        *,
        path_network_io: typing.Optional[Path]=None,
        path_logs_error: typing.Optional[Path]=None,
        cleanup_claim_copies: bool=True,
        output_struct: typing.Optional[spark_types.StructType]=None,
        add_description: bool=True,
        description_dfs: typing.Optional["typing.Mapping[str, pyspark.sql.DataFrame]"]=None,
        **kwargs_eapg
    ) -> "typing.Mapping[str, pyspark.sql.DataFrame]":
    """Execute the EAPG software"""

    assert "claims" in input_dataframes, "'claims' must be in input_dataframes"
    assert "base_table" in input_dataframes, "'base_table' must be in input dataframes"
    if output_struct:
        assert 'upload_template' in kwargs_eapg, \
        "upload_template and output_struct must both be defined"
    if 'upload_template' in kwargs_eapg:
        assert output_struct, "upload_template and output_struct must both be defined"

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
            path_logs_error=path_logs_error,
            cleanup_claim_copies=cleanup_claim_copies,
            **kwargs_eapg
        )
    )
    final_struct = _generate_final_struct(output_struct)
    df_eapg_output = _convert_output_to_df(
        sparkapp,
        rdd_results,
        final_struct,
        )
    sparkapp.save_df(
        df_eapg_output,
        path_output / 'eapgs_claim_level.parquet',
        )
    outputs['eapgs_claim_level'] = df_eapg_output

    LOGGER.info('Transposing EAPG results out to claim line level')
    map_columns_to_keep = OrderedDict(
        [
            ('medicalrecordnumber', 'sequencenumber'),
            ('itemfinaleapg', 'finaleapg'),
            ('itemfinaleapgtype', 'finaleapgtype'),
            ('itemfinaleapgcategory', 'finaleapgcategory'),
        ]
    )

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
        ).fillna(
            {
                column: '0'
                for column in map_columns_to_keep.values()
                }
        )
    df_base_w_eapgs.validate.assert_no_nulls(
        df_base_w_eapgs.columns,
        )

    df_base = _add_description_to_output(
        sparkapp,
        add_description,
        df_base_w_eapgs,
        description_dfs,
    )
    sparkapp.save_df(
        df_base,
        path_output / 'eapgs_claim_line_level.parquet',
        )
    outputs['eapgs_claim_line_level'] = df_base

    if cleanup_claim_copies:
        shutil.rmtree(str(path_workspace))

    return outputs

def _add_description_to_output(
        sparkapp: SparkApp,
        description_bool: bool,
        df_input: "pyspark.sql.DataFrame",
        description_dfs: typing.Optional["typing.Mapping[str, pyspark.sql.DataFrame]"]=None,
    ) -> "pyspark.sql.DataFrame":
    """Adds the description of the EAPG code,category, and """
    if description_bool:
        return _join_description_to_output(
            sparkapp,
            df_input,
            description_dfs,
        )
    else:
        return df_input

def _join_description_to_output(
        sparkapp: SparkApp,
        df_input: "pyspark.sql.DataFrame",
        description_dfs: typing.Optional["typing.Mapping[str, pyspark.sql.DataFrame]"]=None,
    ) -> "pyspark.sql.DataFrame":
    """ Combines Description Dataframes with input"""
    description_dict = eapg.shared.get_descriptions_dfs(sparkapp)
    if description_dfs:
        description_dict.update(description_dfs)

    df_with_eapg = df_input.join(
        spark_funcs.broadcast(description_dict['df_eapgs']),
        ['finaleapg', 'finaleapgtype', 'finaleapgcategory'],
        'left',
    )
    df_with_eapg_type = df_with_eapg.join(
        spark_funcs.broadcast(description_dict['df_eapg_types']),
        ['finaleapgtype'],
        'left',
    )
    df_with_category = df_with_eapg_type.join(
        spark_funcs.broadcast(description_dict['df_eapg_categories']),
        ['finaleapgcategory'],
        'left'
    )
    na_string_fill = 'Claim could not be processed'
    na_number_fill = '0'

    df_output = df_with_category.fillna(
        {
            'eapg_description': na_string_fill,
            'eapg_type_description': na_string_fill,
            'eapg_category_description': na_string_fill,
            'eapg_service_line': na_number_fill,
        }
    )

    return df_output

if __name__ == 'main':
    pass
