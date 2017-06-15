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

import eapg.execution as execution

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================



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

