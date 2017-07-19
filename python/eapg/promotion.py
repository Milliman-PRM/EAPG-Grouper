"""
### CODE OWNERS: Chas Busenburg, Kyle Baird

### OBJECTIVE:
    Automate promotion for EAPG Grouper

### DEVELOPER NOTES:
    <none>
"""
import logging
import os
from pathlib import Path


from indypy.nonstandard.ghapi_tools import repo, conf
from indypy.nonstandard import promotion_tools
from prm.promotion import PipelineVersion
LOGGER = logging.getLogger(__name__)

PATH_RELEASE_NOTES = Path(
    os.environ["EAPG_GROUPER_HOME"]) / 'docs' / 'release-notes.md'

PATH_PROMOTION = Path(r'S:\PRM\Pipeline_Components\EAPG_Grouper')

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:  # pragma: no cover
    """Promotion process for EAPG-Grouper"""
    LOGGER.info('Beginning code promotion for product component')
    github_repo = repo.GithubRepository.from_parts('PRM', 'EAPG-Grouper')
    version = PipelineVersion(
        input("Please enter the version number for this release (e.g. 1.2.3): "),
        partial=True,
    )

    doc_info = promotion_tools.get_documentation_inputs(github_repo)
    release = promotion_tools.Release(github_repo, version, PATH_PROMOTION, doc_info)
    repository_clone = release.export_repo()
    release.make_release_json()
    tag = release.make_tag(repository_clone)
    release.post_github_release(
        conf.get_github_oauth(prompt_if_no_file=True),
        tag,
        body=promotion_tools.get_release_notes(PATH_RELEASE_NOTES, version),
    )
    return 0


if __name__ == '__main__':  # pragma: no cover
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    sys.exit(main())
