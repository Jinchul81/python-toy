cat $1 | awk -F "$2 " '{print $2}' | awk -F "')" '{print $1}' | awk -F ":" '{print $1}' | sort -n | uniq -c
