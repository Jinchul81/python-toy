#!/bin/sh

# This script is to update local address DB
#
# Author: Jinchul (Aug 04, 2017)

CMD_NOW_YEAR="date +%Y"
CMD_NOW_MONTH="date +%Y-%m"
DATE_YYYY=$($CMD_NOW_YEAR)
DATE_YYYYMM=$($CMD_NOW_MONTH)
DATE_YYYY_OF_LAST_MONTH=$(date -d "${DATE_YYYYMM}-15 last month" '+%Y')
DATE_YYYYMM_OF_LAST_MONTH=$(date -d "${DATE_YYYYMM}-15 last month" '+%Y%m')

JUSO_URI="http://www.juso.go.kr/dn.do?reqType=ALLRDNM&regYmd=${DATE_YYYY_OF_LAST_MONTH}&ctprvnCd=00&gubun=RDNM&stdde=${DATE_YYYYMM_OF_LAST_MONTH}&fileName=${DATE_YYYYMM_OF_LAST_MONTH}전체주소%28도로명코드%29_전체분.zip&realFileName=${DATE_YYYYMM_OF_LAST_MONTH}ALLRDNM00.zip"
ARCHIVE_FILENAME="${DATE_YYYYMM_OF_LAST_MONTH}.zip"
SOURCE_DIR="source"

echo "* Creating \"${DATE_YYYYMM_OF_LAST_MONTH}\" directory"
rm -rf ${DATE_YYYYMM_OF_LAST_MONTH}
mkdir -p ${DATE_YYYYMM_OF_LAST_MONTH}
cd ${DATE_YYYYMM_OF_LAST_MONTH}
echo "* Downloading an archive for address CSVs: \"${ARCHIVE_FILENAME}\""
wget ${JUSO_URI} -O ${ARCHIVE_FILENAME}
echo "* Uncompressing the archive for address CSVs: \"${ARCHIVE_FILENAME}\""
unzip -x ${ARCHIVE_FILENAME}
SOURCE_FILE_LIST=`find . -name "*.txt" | awk -F "./" '{print $2}' | grep -e "^build.*txt$"`
for SOURCE_FILE in ${SOURCE_FILE_LIST}
do
  echo "* Converting file format from euc-kr(a.k.a. MS949) to UTF-8: ${SOURCE_FILE}" 
  TMP_FILE="${SOURCE_FILE}.bak"
  iconv --from-code=CP949 --to-code=UTF-8 ${SOURCE_FILE} > ${TMP_FILE}
  mv ${TMP_FILE} ${SOURCE_FILE}
done
cd -
echo "* Creating symbolic link from \"${DATE_YYYYMM_OF_LAST_MONTH}\" to \"${SOURCE_DIR}\""

rm -rf ${SOURCE_DIR}
ln -s ${DATE_YYYYMM_OF_LAST_MONTH} ${SOURCE_DIR}

exit 0
