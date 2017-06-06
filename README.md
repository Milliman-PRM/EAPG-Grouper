## EAPG-Grouper

Library to manage 3M EAPG grouping software execution

## Development guidelines

### Branching strategy

We intend to utilize [git flow](https://datasift.github.io/gitflow/IntroducingGitFlow.html) as the branching strategy for this repository.

### Program headers

Each subfolder in the repository should have an appropriate `ReadMe.md` file describing its purpose according to this [template](https://indy-github.milliman.com/raw/PRM/analytics-pipeline/develop/000_QRM/QRM_Header_Templates/prm_qrm_header.md). Each program should have an appropriate header on the file using this [template](https://indy-github.milliman.com/raw/PRM/analytics-pipeline/develop/000_QRM/QRM_Header_Templates/prm_qrm_header.py).

### Continuous integration

CI has been enabled for this repository. It is currently limited to linting only of python scripts. Unit test requirements are relaxed because of limited nature of logic in python library

### Releases

For documentation purposes, each version of code used to make client deliverables should be tagged and documented as a [release](https://help.github.com/articles/creating-releases/).
