@echo off

SETLOCAL

SET GOPATH=%cd%

REM Build the Sync Gateway service wrapper
go build -o sg-windows.exe sg-service.go

REM Build the Sync Gateway Accelerator service wrapper
go build -o sg-accel-service.exe sg-accel-service.go

ENDLOCAL
