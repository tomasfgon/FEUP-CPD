javac *.java
start rmiregistry
timeout /t 1
start cmd.exe /k java Node 224.0.0.15 88 1 90
timeout /t 1
start cmd.exe /k java Client 1 join
timeout /t 17
start cmd.exe /k java Node 224.0.0.15 88 2 90
timeout /t 1
start cmd.exe /k java Client 2 join
timeout /t 17
start cmd.exe /k java Node 224.0.0.15 88 3 91
timeout /t 1
start cmd.exe /k java Client 3 join
timeout /t 17
java Client 2 put "C:\Users\vmlte\OneDrive\Documentos\FEUP\CPD\cpd\Project 2\testfiles\file.txt"
timeout /t 1
start cmd.exe /k java Node 224.0.0.15 88 4 92
timeout /t 1
start cmd.exe /k java Client 4 join