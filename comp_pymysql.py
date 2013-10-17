"""
Run using python 3.x

Uses the PyMySQL package to access database
"""

import pymysql

conn = pymysql.connect(host = '152.2.15.164' , user = 'charlesczysz' , passwd = 'CharlesSquared', database = 'czysz')

cur = conn.cursor()

fh_match = open("match.txt",'w')
fh_nomatch = open('nomatch.txt','w')
fh_in = open('SNAPResults.txt','r')
fh_dne = open('dne.txt','w') #file to handle SNPs not in either db
linenum = 2

for line in fh_in:

  entries = line.strip().split('\t')

  if 'WARNING' in line or "SNP" in line: #latter removes header line, former lines where rsid doesn't exist in SNAP data
    linenum += 1
    fh_dne.write('\t'.join([entries[0],entries[1] , str(linenum)]) + '\n')
    continue

  #entries = line.strip().split('\t')


  cur.execute("select ld_block from ld_blocks where rsid='" + entries[0] + "' and population='YRI'")

  for row in cur:
    try:
      block1 = row[0]
      
    except IndexError:
      fh_dne.write(entries[0] + "\t" + entries[1] + "\n")
      continue

  cur.execute("select ld_block from ld_blocks where rsid='" + entries[1] + "' and population='YRI'")

  for row in cur:
    try:
      block2 = row[0]
      
    except IndexError:
      fh_dne.write(entries[0] + "\t" + entries[1] + "\n")
      continue


  if block1 == block2:
    fh_match.write('\t'.join([entries[0] , entries[1] , str(block1)]) + '\n')
  else:
    fh_nomatch.write('\t'.join([entries[0] , str(block1) , entries[1] , str(block2)]) + '\n')
