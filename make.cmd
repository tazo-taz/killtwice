@echo off
rem Windows entry point. GNU make is not usually present on Windows, but Git
rem for Windows ships a POSIX sh, so this finds that shell and hands it the
rem exact same scripts the Makefile calls. Usage:
rem
rem     .\make.cmd verify
rem     .\make.cmd verify FAST=1
rem     .\make.cmd seed SEED_ROWS=100000

setlocal enabledelayedexpansion
set "SH="

rem Prefer the sh that ships beside git - deriving it from git's own location
rem handles Git installed somewhere other than Program Files.
for /f "delims=" %%G in ('where git 2^>nul') do (
  if not defined SH (
    for %%D in ("%%~dpG..") do (
      if exist "%%~fD\bin\sh.exe"     set "SH=%%~fD\bin\sh.exe"
      if exist "%%~fD\usr\bin\sh.exe" set "SH=%%~fD\usr\bin\sh.exe"
    )
  )
)

if not defined SH if exist "%ProgramFiles%\Git\bin\sh.exe" set "SH=%ProgramFiles%\Git\bin\sh.exe"
if not defined SH if exist "%ProgramFiles(x86)%\Git\bin\sh.exe" set "SH=%ProgramFiles(x86)%\Git\bin\sh.exe"

rem Deliberately not falling back to `where sh` or `where bash`: on Windows
rem those commonly resolve to System32\bash.exe, which is the WSL launcher and
rem sees a different filesystem than the one this repo is checked out on.

if not defined SH (
  echo make.cmd: no POSIX shell found.
  echo Install Git for Windows ^(it ships one^), or run the scripts under WSL.
  exit /b 1
)

"%SH%" "%~dp0scripts/run.sh" %*
exit /b %ERRORLEVEL%
