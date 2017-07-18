"""
### CODE OWNERS: Chas Busenburg
### OBJECTIVE:
    Test promotion.py
### DEVELOPER NOTES:
"""
import logging
import sys

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def test_import(): # pylint: disable=wrong-import-position
    """ test that eapg.promotion imports properly"""
    import eapg.promotion

    assert 'eapg.promotion' in sys.modules





