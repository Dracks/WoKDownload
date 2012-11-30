#!/bin/sh

#mysql -uroot -proot isi -B -e "select timescited, count(*) from \`node\` where year=2010 group by timescited order by timescited;" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > mysql_exported_table.csv
mysql -uwebofscience -p webofscience -B -e "$1" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > $2