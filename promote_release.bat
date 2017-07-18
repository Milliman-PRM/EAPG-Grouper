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
call %~dp0setup_env.bat
echo Finished setting up promotion environment for product component

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling promotion script
python -m promotion
