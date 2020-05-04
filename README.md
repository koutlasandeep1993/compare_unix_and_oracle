# compare_unix_and_oracle
 This python will compare the data between file in unix and oracle database

# Configurations:
 This python should be called by cmd promt as we are passing argument.please find below cmd
 #python "C:\compare_2_files.py" --SRC C:\file_1.txt --TGT C:\file_2.txt --TGT-SRC C:\tgt_not_src.csv --SRC-TGT C:\src_not_tgt.csv
 
 file_1.txt: is file from unix server
 file_2.txt:  output of oracle query in file
 tgt_not_src.csv: records in oracle and not in unix file
 src_not_tgt.csv: records in unix file and not in oracle
 
 # about script
 
 1. This script will connect to unix server and download the mentioned file.
 2. Read the query from file_compare_info.json and run the same in oracle system.
 3. output of oracle query is stored in file.
 4. both the files are than compared.
 

 
 
