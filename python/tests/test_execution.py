"""
### CODE OWNERS: Chas Busenburg, Ben Copeland
### OBJECTIVE:
    Unit test for execution.py
### DEVELOPER NOTES:
    None at time.
"""
# Expecting to test private pieces of module.
# pylint: disable=protected-access


from pathlib import Path
import filecmp

import pytest
import pyspark.sql.functions as spark_funcs

import eapg.execution as execution
import eapg
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_THIS_FILE = Path(__file__).parent
except NameError:
    _PATH_PARTS = list(Path(execution.__file__).parent.parts)
    _PATH_PARTS[-1] = "tests"
    _PATH_THIS_FILE = Path(*_PATH_PARTS) # pylint: disable=redefined-variable-type


PATH_MOCK_DATA = _PATH_THIS_FILE / 'mock_data'
PATH_MOCK_SCHEMAS = _PATH_THIS_FILE / 'mock_schemas'


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



def test__public_log_parameters(tmpdir):
    """test that public_log_check tests correctly"""
    id_partition = 3
    true_test_path = Path(str(tmpdir))
    false_test_path = None

    expected_true_dict = {
        'error_log': true_test_path / 'error_log_{}.txt'.format(id_partition),
        'edit_log': true_test_path / 'edit_log_{}.txt'.format(id_partition)
    }

    expected_false_dict = dict()
    result_true_dict = execution._get_public_log_parameters(
        id_partition,
        true_test_path,
    )
    result_false_dict = execution._get_public_log_parameters(
        id_partition,
        false_test_path,
    )
    assert expected_false_dict == result_false_dict
    assert expected_true_dict == result_true_dict

def test__compose_cli_parameters(tmpdir):
    """test _compose_options, specifically that **kwargs outputs as expected"""
    id_partition = 3
    path_input_file = Path(str(tmpdir)) / 'temp_in.txt'
    path_output_file = Path(str(tmpdir)) /  'temp_out.txt'
    path_input_template = execution.PATH_INPUT_TEMPLATE
    path_output_template = execution.PATH_OUTPUT_TEMPLATE
    eapg_version = execution.EAPG_VERSION
    path_logs_public = None

    output_sans_kwargs = {
        'input': path_input_file.as_posix(),
        'input_template': path_input_template.as_posix(),
        'upload': path_output_file.as_posix(),
        'upload_template': path_output_template.as_posix(),
        'input_header': 'off',
        'schedule': 'off',
        'grouper': eapg_version,
        'input_date_format': 'yyyy-MM-dd',
    }
    kwargs = {
        'test': True,
    }
    output_kwargs = {**output_sans_kwargs, **kwargs}
    assert output_sans_kwargs == execution._compose_cli_parameters(
        id_partition,
        path_input_file,
        path_output_file,
        path_logs_public,
    )
    assert output_kwargs == execution._compose_cli_parameters(
        id_partition,
        path_input_file,
        path_output_file,
        path_logs_public,
        test=True,
    )
def test__compose_eapg_subprocess_args(): # pylint: disable=invalid-name
    """test subprocess correctly creates array"""
    id_partition = 42
    options = {
        'input': 'test_string_input',
        'input_template': 'test_string_input_template',
        }
    args = [
        str(execution.PATH_EAPG_GROUPER),
        '-input',
        'test_string_input',
        '-input_template',
        'test_string_input_template']
    test_out = execution._compose_eapg_subprocess_args(
        id_partition,
        options,
    )
    test_odd_index = test_out[1::2]
    assert not set(test_out) - set(args) #must contain all elements
    assert args[0] == test_out[0] #first argument must be the EAPG_GROUPER path
    assert all(item.startswith('-') for item in test_odd_index) #odd-index must start with '-'

def test__run_eapg_subprocess(tmpdir):
    """test the eapg_subprocess on a single .csv file"""
    path_input = PATH_MOCK_DATA / 'execution_eapg_in.csv'
    path_upload = Path(str(tmpdir)) / 'execution_eapg_out.csv'


    options = {
        'input':  path_input.as_posix(),
        'input_template' : execution.PATH_INPUT_TEMPLATE.as_posix(),
        'upload': path_upload.as_posix(),
        'upload_template': execution.PATH_OUTPUT_TEMPLATE.as_posix(),
        'input_header': 'on',
        'schedule': 'off',
        'grouper': execution.EAPG_VERSION,
        'input_date_format': 'yyyy-MM-dd',
    }
    id_partition = 42
    output_expected_path = PATH_MOCK_DATA / 'execution_eapg_out.csv'
    output_test_path = path_upload
    execution._run_eapg_subprocess(
        id_partition,
        options,
    )
    assert filecmp.cmp(
        str(output_expected_path),
        str(output_test_path),
        shallow=False,
    )  # The two files must be exactly the same.

def test__assign_path_workspace(tmpdir):
    """test assign path workspace """
    path_output = Path(str(tmpdir))
    none_workspace = path_output / '_temp_eapg_grouper'
    path_network_io = Path('C:/test')

    assert execution._assign_path_workspace(
        None,
        path_output,
    ) == none_workspace

    assert execution._assign_path_workspace(
        path_network_io,
        path_output,
    ) == path_network_io

def test__generate_final_struct(mock_schemas): # pylint: disable=redefined-outer-name
    """ test that a final struct is correctly created"""

    none_final_struct = build_structtype_from_csv(
        eapg.shared.PATH_SCHEMAS / 'eapgs_out.csv'

    )
    output_final_struct = mock_schemas['member']

    assert execution._generate_final_struct(None) == none_final_struct
    assert execution._generate_final_struct(output_final_struct) == output_final_struct

def test_run_eapg_grouper(
        spark_app,
        tmpdir
    ):
    """ test the running of eapg_grouper"""
    input_path = PATH_MOCK_DATA / 'execution_eapg_in.csv'
    output_data_path = Path(str(tmpdir))
    input_struct = build_structtype_from_csv(
        eapg.shared.PATH_SCHEMAS / 'eapgs_in.csv'
    )
    output_struct = build_structtype_from_csv(
        eapg.shared.PATH_SCHEMAS /'eapgs_out.csv'
    )
    output_struct_claim_line = build_structtype_from_csv(
        eapg.shared.PATH_SCHEMAS /'eapgs_out_claim_line.csv'
    )
    df_input_data = spark_app.session.read.csv(
        str(input_path),
        schema=input_struct,
        header=True,
        mode="FAILFAST",
    )
    base_table = df_input_data.select(
        spark_funcs.explode(
            spark_funcs.split(
                spark_funcs.col('medicalrecordnumber'), #pylint: disable=no-member
                ';'
                )
            ).alias('sequencenumber')
        )
    input_dataframes = {
        'claims': df_input_data,
        'base_table': base_table,
        }
    df_output_data_claim = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'execution_eapg_out.csv'),
        schema=output_struct,
        header=True,
        mode="FAILFAST",
    )
    df_output_data_claim_line = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'execution_eapg_out_claim_line.csv'),
        schema=output_struct_claim_line,
        header=True,
        mode="FAILFAST",
    )

    df_eapg_output = execution.run_eapg_grouper(
        spark_app,
        input_dataframes,
        output_data_path,
    )
    df_wide_misses = df_eapg_output['eapgs_claim_level'].subtract(
        df_output_data_claim
        )
    assert df_wide_misses.count() == 0
    df_long_misses = df_eapg_output['eapgs_claim_line_level'].subtract(
        df_output_data_claim_line
        )
    assert df_long_misses.count() == 0

    path_logs = output_data_path / 'logs'
    path_logs.mkdir(exist_ok=True)
    execution.run_eapg_grouper(
        spark_app,
        input_dataframes,
        output_data_path,
        path_logs_public=path_logs,
    )

    assert (path_logs / 'error_log_0.txt').exists()
    assert (path_logs / 'edit_log_0.txt').exists()

def test__add_description_to_output(
        spark_app,
    ):
    """test function that checks description output to test files."""
    input_path = PATH_MOCK_DATA / 'execution_eapg_out_claim_line.csv'
    input_struct = build_structtype_from_csv(
        PATH_MOCK_SCHEMAS / 'execution_eapg_out_claim_line.csv'
    )
    df_input_data = spark_app.session.read.csv(
        str(input_path),
        schema=input_struct,
        header=True,
        mode="FAILFAST",
    )
    df_empty_false = df_input_data.subtract(
        execution._add_description_to_output(
            spark_app,
            False,
            df_input_data,
        )
    )
    assert not df_empty_false.count() #if add_description_bool is False should return the same

    output_path = PATH_MOCK_DATA / 'execution_eapg_out_claim_description.csv'
    output_struct_path = PATH_MOCK_SCHEMAS / 'schema_eapg_desc_out.csv'
    output_struct = build_structtype_from_csv(output_struct_path)
    df_output = spark_app.session.read.csv(
        str(output_path),
        schema=output_struct,
        header=True,
    )
    df_true = df_output.subtract(
        execution._add_description_to_output(
            spark_app,
            True,
            df_input_data,
        )
    )
    assert not df_true.count()

def test__join_description_to_output(spark_app):
    """ test the joining of the description dfs to an output"""
    path_test_schema = PATH_MOCK_SCHEMAS / 'schema_eapg_desc_out.csv'
    test_schema = build_structtype_from_csv(path_test_schema)
    df_test = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'execution_eapg_out_claim_description.csv'),
        schema=test_schema,
        header=True,
    )

    path_input_schema = PATH_MOCK_SCHEMAS / 'execution_eapg_out_claim_line.csv'
    input_schema = build_structtype_from_csv(path_input_schema)

    df_input = spark_app.session.read.csv(
        str(PATH_MOCK_DATA / 'execution_eapg_out_claim_line.csv'),
        schema=input_schema,
        header=True,
    )

    df_output = execution._join_description_to_output(
        spark_app,
        df_input,
    )

    count = df_test.subtract(df_output).count()

    assert not count #Test and input do not match
