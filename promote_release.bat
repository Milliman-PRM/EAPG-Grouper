@echo off
rem ### CODE OWNERS: Chas Busenburg
rem 
rem ### OBJECTIVE:
rem   Run the promotion process to promote a new version of this component
rem 
rem ### DEVELOPER NOTES:
rem  *none*


rem LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

call "%~dp0setup_env.bat"

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling promotion script
python -m eapg.promotion
