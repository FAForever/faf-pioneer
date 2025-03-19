@echo off

:: Change directory to ProgramData
pushd "%ProgramData%\FAForever\bin"

if "%1%"=="1" goto session1
if "%1%"=="2" goto session2

echo Unknown user session ID, use 1 or 2 as "start_client.bat 1".
goto end

:session1
ForgedAlliance.exe ^
    /init init.lua ^
    /name UserA ^
    /country NL ^
    /nobugreport ^
    /gpgnet 127.0.0.1:21000 ^
    /numgames 11 ^
    /log "%ProgramData%\FAForever\logs\ice_test_uid1.log"
goto end

:session2
ForgedAlliance.exe ^
    /init init.lua ^
    /name UserB ^
    /country NL ^
    /nobugreport ^
    /gpgnet 127.0.0.1:21001 ^
    /numgames 11 ^
    /log "%ProgramData%\FAForever\logs\ice_test_uid2.log"

:end
:: Clear current directory back to original
popd
