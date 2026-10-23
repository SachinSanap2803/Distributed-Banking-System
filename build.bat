@echo off
echo ===================================================
echo Compiling Distributed Banking System (Java RMI)...
echo ===================================================
javac *.java
if %errorlevel% neq 0 (
    echo [ERROR] Compilation failed!
    pause
    exit /b %errorlevel%
)
echo [SUCCESS] All Java classes compiled successfully!
pause
