import cx_Oracle
import csv
import os
import sys
import pysftp
import json

###############Sample how to run this script##########

#python "C:\compare_2_files.py" --SRC C:\file_1.txt --TGT C:\file_2.txt --TGT-SRC C:\tgt_not_src.csv --SRC-TGT C:\src_not_tgt.csv

#########Configurations########
src_file_path=sys.argv[2]
tgt_file_path=sys.argv[4]
records_tgt_not_in_src_file_path=sys.argv[6]
records_src_not_in_tgt_file_path=sys.argv[8]
#########ftp config
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
########reading json
with open ("Z:\\file_compare_info.json") as data_file:
  data=json.load(data_file)
data_file.close()

#########ftp details######


ftp_host=data["Para"][0]["ftp_host"]
path=data["Para"][0]["ftp_file"]
ftp_user=data["Para"][0]["ftp_user"]
ftp_passwd=data["Para"][0]["ftp_passwd"]


#############Oracle connection details#####

conn_str=data["Para"][0]["oracle_conn_str"]
sql=data["Para"][0]["oracle_sql"]
cnopts.hostkeys = None
#############get the file from LINUX

with pysftp.Connection(ftp_host, username=ftp_user, password=ftp_passwd,cnopts=cnopts) as sftp:
    sftp.get(path,src_file_path)

print ("file recieved from linux server")

print ("****************FTP FILE TRANSFER STEP DONE**************")


########Connecting source oracle system###########

conn=cx_Oracle.connect(conn_str,encoding = "UTF-8")
c=conn.cursor()
data_out=c.execute(sql)
print("hello")


#########File format of the file to be generated from oracle#######
f = open(tgt_file_path, "w",encoding='utf-8',errors='ignore')
writer = csv.writer(f, lineterminator="\n", quoting=csv.QUOTE_NONE,delimiter='\022',escapechar='\\',quotechar="\022")

col_names = [row[0] for row in c.description]
header=''.join(col_names)
f.write(header)
f.write('\n')
for line in data_out:
    writer.writerow(line)
f.close()



src_file=open(src_file_path,"r",encoding='utf-8',errors='ignore')
tgt_file=open(tgt_file_path,"r",encoding='utf-8',errors='ignore')
records_tgt_not_in_src_file=open(records_tgt_not_in_src_file_path,"w",encoding='utf-8',errors='ignore')
records_tgt_not_in_src=[]
records_src_not_in_tgt_file=open(records_src_not_in_tgt_file_path,"w",encoding='utf-8',errors='ignore')
records_src_not_in_tgt=[]


########Reading source and target files############

src_file_list= [line.replace(" ","").replace('"','') for line in src_file.readlines()]
number_of_line_src_file=len(src_file_list)
tgt_file_list_1=[line.replace(" ","") for line in tgt_file.readlines()]
tgt_file_list=[line.replace("\\\\",'\\') for line in tgt_file_list_1]
number_of_line_tgt_file=len(tgt_file_list)



##########comparing counts###############


print("*********Testing stats************")

print("Part #1: The count Check")
print(" number of records in source file is :")
print (number_of_line_src_file)
print(" number of records in target file is :")
print (number_of_line_tgt_file)
if(number_of_line_tgt_file-number_of_line_src_file)==0:
    print("Part #1 Check Completed and number of records in source and target files matches")
else:
    print("Part #1 Check Completed and number of records in source and target files DOES NOT MATCH")
###################comparing records/data########################


print("*********************************")
print("Part #2: Data check between source and target file")
#####################target Minus source##############
records_tgt_not_in_src.append(set(tgt_file_list) - set(src_file_list))
if len(records_tgt_not_in_src[0])==0:
    print("All records of target are in source")
else:
        print("!!!ERROR OCCURED")
        print("Some records of target file are not present in source file")
        for record in records_tgt_not_in_src:
            records_tgt_not_in_src_file.write(",".join(list(record)))

#####################Source Minus Target##############
records_src_not_in_tgt.append(set(src_file_list) - set(tgt_file_list))
if len(records_src_not_in_tgt[0])==0:
    print("All records of Source are in Target")
else:
        print("!!!!ERROR OCCURED")
        print("Some records of source file are not present in target file")
        for record_1 in records_src_not_in_tgt:
            records_src_not_in_tgt_file.write(",".join(list(record_1)))

if len(records_src_not_in_tgt[0])==0 and len(records_tgt_not_in_src[0])==0:
    print("Both source and target files matched on both count and data")

print("Part#2 Check completed")
print("********************************")
###########Closing all files################
src_file.close()
tgt_file.close()
records_src_not_in_tgt_file.close()
records_tgt_not_in_src_file.close()
###################Ending script###################
print(" Script execution completed")


