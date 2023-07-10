@echo off

set YCSB_PATH=C:\Users\aliso\OneDrive\Documents\NetBeansProjects\YCSB-master
set WORKLOAD_PATH=workloads

set /A NUM_RUNS=10

echo Iniciando...

for %%t in (mista) do (
    for %%w in (workload100000) do (
        for %%d in (jdbc mongodb redis neo4j) do (
            for %%i in (1) do (
                for %%a in (um) do (
                    echo Running YCSB benchmark for %%d with %%w (Run %%i) 
                    call bin\ycsb run %%d -P %WORKLOAD_PATH%\%%t\%%w -P %%d\src\main\config\db.properties >> "C:\Users\aliso\Documents\100000\sequencial\%%t\%%d_%%w_%%i.txt"
                    call bin\ycsb run %%d -P %WORKLOAD_PATH%\%%t\%%w -P %%d\src\main\config\db.properties -threads 5 >> "C:\Users\aliso\Documents\100000\thread\%%t\%%d_%%w_%%i.txt"
                    echo YCSB benchmark completed for %%d with %%w - Run %%i 
                )
            )
        )
    )
)



echo All YCSB benchmarks completed!
PAUSE
