
::python serv.py --port=8001

for /l %%i in (8001,1,8002) do (start HIDECMD.EXE servs.bat %%i)

::start redis\redis-server

::start nginx\nginx.exe

