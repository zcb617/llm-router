@echo off
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":38888 .*LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
    echo 已停止代理进程 (PID: %%a)
    goto :end
)
echo 未找到运行中的代理进程
:end
timeout /t 2 >nul
