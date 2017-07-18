@echo off
rem ### CODE OWNERS: Chas Busenburg
rem 
rem ### OBJECTIVE:
rem   Run the promotion process to promote a new version of this component
rem 
rem ### DEVELOPER NOTES:
rem  *none*


rem LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Setting up promotion environment for product component
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Running from %~f0

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling latest pipeline-components-env file
call "S:\PRM\Pipeline_Components_Env\pipeline_components_env.bat"

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Redirecting component HOME to local copy
set EAPG_GROUPER_HOME=%~dp0
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: EAPG_GROUPER_HOME is now %EAPG_GROUPER_HOME%

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Prepending local python library to PYTHONPATH
set PYTHONPATH=%EAPG_GROUPER_HOME%\python;%PYTHONPATH%
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: PYTHONPATH is now %PYTHONPATH%

echo Finished setting up promotion environment for product component

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling promotion script
python -m promotion
