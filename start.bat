@echo off/hid
echo "INIT: 05-TRISKIN-TEAM-TBOARD-ETL-QUADRO-RHSENIOR-main"

rem taskkill /f /im chrome.exe
rem taskkill /f /im chromedriver.exe

rem C:/Users/mis/AppData/Local/Programs/Python/Python310/python.exe "C:\Users\mis\Desktop\PROCESSOS_ETL\01-TRISKLE-TEAM-WINOVER_GOLD_OLTP_BREAKLIST\main.py" 

set BASE_DIR=C:\Users\mis\Desktop\PROCESSOS_ETL\05-TRISKIN-TEAM-TBOARD-ETL-QUADRO-RHSENIOR\
set VENV_DIR=%BASE_DIR%\.vscode

call "%VENV_DIR%\Scripts\activate"

python "%BASE_DIR%\main.py"

deactivate

echo "END:05-TRISKIN-TEAM-TBOARD-ETL-QUADRO-RHSENIOR-main"


exit



