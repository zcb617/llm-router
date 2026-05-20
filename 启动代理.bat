@echo off
title LLM Router Proxy
cd /d "%~dp0"

echo =========================================
echo   LLM Router Proxy
echo   Close this window to stop the proxy
echo =========================================
echo.

python start.py

echo.
echo Proxy stopped.
pause