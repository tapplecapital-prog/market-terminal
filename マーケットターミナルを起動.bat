@echo off
rem ============================================================
rem  APPLE CAPITAL MARKET TERMINAL - one-click launcher
rem  サーバを隠れて起動し（多重起動しない）、準備でき次第ブラウザを開く
rem ============================================================
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; if(-not (Get-NetTCPConnection -LocalPort 8799 -State Listen)){ $e=(Get-Command pythonw).Source; if(-not $e){ $e=(Get-Command python).Source }; if(-not $e){ $e='python' }; Start-Process -FilePath $e -ArgumentList 'server.py','8799' -WorkingDirectory (Get-Location).Path -WindowStyle Hidden; for($i=0;$i -lt 40;$i++){ Start-Sleep -Milliseconds 300; if(Get-NetTCPConnection -LocalPort 8799 -State Listen){ break } } }; Start-Process 'http://127.0.0.1:8799/'"
exit /b
