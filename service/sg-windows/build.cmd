@echo off

SETLOCAL

set CWD=%cd%
cd ..\..\..\..\..\..
set GO_PATH=%cd%
cd %CWD%
SET GOPATH=%cd%;%GO_PATH%

REM Build the Sync Gateway service wrapper
go build -o sg-windows.exe sg-service\sg-service.go

ENDLOCAL
