@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0close_ticket.ps1" %*
