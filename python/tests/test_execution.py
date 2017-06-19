"""
### CODE OWNERS: Chas Busenburg
### OBJECTIVE:
    Unit test for execution.py
### DEVELOPER NOTES:
    None at time.
"""
# Expecting to test private pieces of module.
# pylint: disable=protected-access
from pathlib import Path
import filecmp

import eapg.execution as execution

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
try:
    MOCK_DATA_PATH = Path(__file__).parent / 'mock_data'
    MOCK_SCHEMAS_PATH = Path(__file__).parent / 'mock_schemas'
except NameError:
    pass



def test__public_log_check(tmpdir):
    """test that public_log_check tests correctly"""
    id_partition = 3
    true_test_path = Path(str(tmpdir))
    false_test_path = None

    true_dict = {
        'test' : True
    }
    true_after_dict = {
        'test' : True,
        'error_log': true_test_path / 'error_log_{}.txt'.format(id_partition),
        'edit_log': true_test_path / 'edit_log_{}.txt'.format(id_partition)
    }

    false_dict = {
        'test' : False
    }
    assert false_dict == execution._public_log_check(id_partition,
                                                     false_test_path,
                                                     false_dict)

    assert true_after_dict == execution._public_log_check(id_partition,
                                                          true_test_path,
                                                          true_dict)

def test__compose_options(tmpdir):
    """test _compose_options, specifically that **kwargs outputs as expected"""
    id_partition = 3
    path_input_file = Path(str(tmpdir)) / 'temp_in.txt'
    path_output_file = Path(str(tmpdir)) /  'temp_out.txt'
    path_input_template = execution.PATH_INPUT_TEMPLATE
    path_output_template = execution.PATH_OUTPUT_TEMPLATE
    eapg_version = execution.EAPG_VERSION
    path_logs_public = None
    output_kwargs = {
        'input': path_input_file.as_posix(),
        'input_template': path_input_template.as_posix(),
        'upload': path_output_file.as_posix(),
        'upload_template': path_output_template.as_posix(),
        'input_header': 'off',
        'schedule': 'off',
        'grouper': eapg_version,
        'input_date_format': 'yyyy-MM-dd',
        'test':True
    }

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

    assert output_sans_kwargs == execution._compose_options(id_partition,
                                                            path_input_file,
                                                            path_output_file,
                                                            path_logs_public)
    assert output_kwargs == execution._compose_options(id_partition,
                                                       path_input_file,
                                                       path_output_file,
                                                       path_logs_public,
                                                       test=True)
def test__subprocess_array_create():
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
    test_out = execution._subprocess_array_create(id_partition, options)
    test_odd_index = test_out[1::2]
    assert len(set(test_out) - set(args)) == 0 #must contain all elements
    assert args[0] == test_out[0] #first argument must be the EAPG_GROUPER path
    assert all(item.startswith('-') for item in test_odd_index) #odd-index must start with '-'

def test__subprocess_partition(tmpdir):
    path_input = MOCK_DATA_PATH / 'execution_eapg_in.csv'
    path_upload = Path(str(tmpdir)) / 'execution_eapg_out.csv'


    options = {
        'input':  path_input.as_posix(),
        'input_template' : execution.PATH_INPUT_TEMPLATE.as_posix(),
        'upload': path_upload.as_posix(),
        'upload_template': execution.PATH_OUTPUT_TEMPLATE.as_posix(),
        'input_header': 'off',
        'schedule': 'off',
        'grouper': execution.EAPG_VERSION,
        'input_date_format': 'yyyy-MM-dd',
    }
    id_partition = 42
    output_expected_path =  MOCK_DATA_PATH/ 'execution_eapg_out.csv'
    output_test_path = path_upload
    execution._subprocess_partition(id_partition, options)
    assert filecmp.cmp(str(output_expected_path),
                       str(output_test_path))  # compare basic stats for the two files.
 
