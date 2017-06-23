"""
### CODE OWNERS: Ben Copeland, Chas Busenburg

### OBJECTIVE:
  Test shared functions for EAPG grouper

### DEVELOPER NOTES:
  <none>
"""
# Ignore some design quirks imposed by pytest
# pylint: disable=redefined-outer-name
from pathlib import Path

import pytest

import pyspark.sql.functions as spark_funcs
from pyspark.sql import Column

import eapg.shared
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_THIS_FILE = Path(__file__).parent
except NameError:
    _PATH_PARTS = list(Path(eapg.shared.__file__).parent.parts)
    _PATH_PARTS[-1] = "tests"
    _PATH_THIS_FILE = Path(*_PATH_PARTS) # pylint: disable=redefined-variable-type

PATH_MOCK_DATA = _PATH_THIS_FILE / "mock_data"
PATH_MOCK_SCHEMAS = _PATH_THIS_FILE / "mock_schemas"

# pylint: disable=no-member, protected-access

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_name_from_path(path):
    """Derive data set name from testing file path"""
    return path.stem[path.stem.find("_") + 1:]


@pytest.fixture
def mock_schemas():
    """Schemas for testing data"""
    return {
        _get_name_from_path(path): build_structtype_from_csv(path)
        for path in PATH_MOCK_SCHEMAS.glob("shared_*.csv")
        }


@pytest.fixture
def mock_dataframes(spark_app, mock_schemas):
    """Dataframes of testing data"""
    return {
        _get_name_from_path(path): spark_app.session.read.csv(
            str(path),
            schema=mock_schemas[_get_name_from_path(path)],
            header=True,
            mode="FAILFAST",
            )
        for path in PATH_MOCK_DATA.glob("shared_*.csv")
        }


def test__get_icd_columns(mock_dataframes):  # pylint: disable=invalid-name
    """Test the collection of ICD columns"""
    df_diags = mock_dataframes['icd_columns']
    df_diags_less_cols = df_diags.select(
        'sequencenumber',
        'diag_field_1',
        'diag_field_2',
        'diag_field_3',
        )

    icd_columns = eapg.shared._get_icd_columns(
        df_diags,
        'diag_field_',
        )
    icd_columns_less_cols = eapg.shared._get_icd_columns(
        df_diags_less_cols,
        'diag_field_',
        )

    assert isinstance(icd_columns[0], Column)
    assert len(icd_columns) == eapg.shared.N_ICD_COLUMNS

    assert len(icd_columns_less_cols) == 3

    # Test that grouping by sequencenumber (unique) returns the same dataframe
    icd_columns_grouped = eapg.shared._get_icd_columns(
        df_diags,
        'diag_field_',
        from_grouped_dataframe=True,
        )

    df_diags_from_grouped = df_diags.groupBy(
        'sequencenumber',
    ).agg(
        *icd_columns_grouped
    )

    assert df_diags.select(
        df_diags_from_grouped.columns
    ).subtract(df_diags_from_grouped).count() == 0


def test__convert_poa_to_character(mock_dataframes):  # pylint: disable=invalid-name
    """Test the conversion of POA columns to character values"""
    df_poas = mock_dataframes['poa_columns']

    df_poas_character = df_poas.select(
        *[
            eapg.shared._convert_poa_to_character(
                spark_funcs.col(column)
                ).alias(column)
            for column in df_poas.columns
            if column != 'sequencenumber'
            ]
        )

    # Test that all null POAs end up as 'N'
    assert df_poas_character.dropna().count() == df_poas.count()

    # Test that all values are valid
    miss_filter = spark_funcs.lit(False)
    for column in df_poas_character.columns:
        miss_filter = miss_filter & (
            ~spark_funcs.col(column).isin('Y', 'N', 'W', 'U')
            )
    assert df_poas_character.filter(miss_filter).count() == 0


def test__coalesce_metadata_and_cast(
        mock_dataframes,
        mock_schemas,
    ):  # pylint: disable=invalid-name
    """Test the coalescing of metadata and casting"""
    dataframe_orig = mock_dataframes['for_cast']
    target_schema = mock_schemas['casted']

    dataframe_final = eapg.shared._coalesce_metadata_and_cast(
        target_schema,
        dataframe_orig,
        )

    # Test that all fields match the target schema
    assert all(
        dataframe_final.schema[field.name].dataType == field.dataType
        for field in target_schema
        )

    # Test that all fields contain metadata from the original dataframe and struct
    assert all(
        len(dataframe_final.schema[field.name].metadata) == 2
        for field in target_schema
        )

    # Test that dataframe metadata overrides struct metadata
    dataframe_modified_metadata = eapg.shared._coalesce_metadata_and_cast(
        target_schema,
        dataframe_orig.withColumn(
            'sequencenumber',
            spark_funcs.col('sequencenumber').aliasWithMetadata(
                'sequencenumber',
                metadata={'label': 'Claim Record', 'subdelimited': False}
                )
            ),
        )
    assert dataframe_modified_metadata.schema['sequencenumber'].metadata['subdelimited'] != \
        target_schema['sequencenumber'].metadata['subdelimited']


def test_get_standard_inputs_from_prm(
        mock_dataframes,
    ):  # pylint: disable=invalid-name
    """Test the coalescing of metadata and casting"""
    input_dataframes = {
        'outclaims_prm': mock_dataframes['raw_input'],
        'member': mock_dataframes['member'],
        }
    df_results = eapg.shared.get_standard_inputs_from_prm(
        input_dataframes,
        )

    def _round_float_cols(
            dataframe
        ):
        """Remove all float columns from a dataframe because comparing floats goes poorly"""
        col_types = {col.name: col.dataType.simpleString() for col in dataframe.schema.fields}

        col_select = list()
        for name in dataframe.columns:
            if col_types[name] in {"float", "double"}:
                col_select.append(
                    spark_funcs.format_number(
                        spark_funcs.col(name),
                        2,
                        ).alias(name)
                    )
            else:
                col_select.append(name)

        return dataframe.select(col_select)

    # Test after rounding floats and forcing string columns to empty strings
    assert _round_float_cols(df_results['claims'].fillna('')).subtract(
        _round_float_cols(mock_dataframes['expected_input'].fillna(''))
        ).count() == 0

    assert df_results['base_table'].subtract(
        mock_dataframes['base_table']
        ).count() == 0

