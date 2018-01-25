# # 기지국 메타 데이터 생성a
# * 요약
# : 기지국 ru_name에서 지하철/아파트/인빌딩 정보 추출

## 스파크 연결 ####
library(sparklyr)
library(DBI)
library(RJDBC)
# SET SPARK HOME
Sys.setenv(SPARK_HOME = "/usr/hdp/current/spark-client")

## 연결 configuration
conf <- spark_config()
conf[["spark.executor.instances"]] <- 4
conf[["spark.executor.cores"]] <- 8
conf[["spark.executor.memory"]] <- '8G'
conf[["spark.yarn.am.memory"]] <- '4G'
conf[["spark.yarn.am.cores"]] <- 2

# 연결
sc <- spark_connect(master = "yarn-client", config=conf)
#
# ## hive 연결 ####
# options( java.parameters = "-Xmx64g" )
# library(rJava)
#
# cp = c("/usr/hdp/current/hive-client/lib/hive-jdbc.jar",
#        "/usr/hdp/current/hadoop-client/hadoop-common.jar")
# .jinit(classpath=cp)
# drv <- JDBC("org.apache.hive.jdbc.HiveDriver",
#             "/usr/hdp/current/hive-client/lib/hive-jdbc.jar",
#             identifier.quote="`")
# conn <- dbConnect(drv, "jdbc:hive2://10.40.89.94:10000", "dbpuser4", "dbpuser4")

# dbSendQuery(conn ,"use new_loc")

## 1. 데이터 불러오기 / 변수 타입 지정
library(readr)
library(dplyr)
library(magrittr)

# Arguments
args = commandArgs(trailingOnly=TRUE)

# 1.1 데이터 불러오기 ####
df <- dbGetQuery(sc, 'select * from src.xls_cell_meta_raw')
#df <- dbGetQuery(sc, 'select * from shyi.cms_raw_gv where dt=20170401')


# 1.2 변수 타입 지정 ####
df <- df%>%
  mutate(c_uid = as.character(c_uid),
         enb_id = as.character(enb_id),
         cell_id = as.character(cell_id),
         city_id = as.character(city_id),
         city_id2 = as.character(city_id2),
         dong_id = as.character(dong_id),
         block_cd = as.character(block_cd))

# 1.3 unique한 테이블로 압축 ####
df <- df%>%distinct()

# 1.4 unique ID 생성 #로그데이터 기준으로 c_uid와 cell_id만 남김 ####
## Unique한 행만 남김
df <- df%>%
  mutate(id = paste(c_uid, cell_id, sep="_"))



## 2. 메타 정보 추출 ####
getParenthesis <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\([^\\)|^\\(]+\\)',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), paste, collapse=' '))
  tmp[tmp==""] <- NA
  return(tmp)}

# [ ] 안 키워드 추출
getBracket <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\[[^]|^[]+\\]',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), paste, collapse=' '))
  tmp[tmp==""] <- NA
  return(tmp)}

# ( ), [ ] 안 키워드 제거
rmMeta <- function(x){
  x <- gsub(' ', '', x)
  tmp <- gsub('\\([^\\)|^\\(]+\\)','',  x)
  tmp <- gsub('\\[[^]|^[]+\\]','', tmp)
  tmp <- gsub('BR_','', tmp)
  tmp <- gsub('PR_','', tmp)
  return(tmp)
}

# 지하철 관련 키워드 추출
getTrain <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\(철\\)|\\(철2\\)|\\(철K\\)|\\(철K2\\)|\\(지\\)',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), function(x) paste(sort(unique(x)), collapse=' ')))
  tmp[tmp==""] <- NA
  return(tmp)
}

# 아파트 관련 키워드 추출
getApt <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\(아\\)',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), function(x) paste(sort(unique(x)), collapse=' ')))
  tmp[tmp==""] <- NA
  return(tmp)
}

# 주차장 관련 키워드 추출
getParkinglot <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\(주\\)',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), function(x) paste(sort(unique(x)), collapse=' ')))
  tmp[tmp==""] <- NA
  return(tmp)
}

# 인빌딩 관련 키워드 추출
getInbuilding <- function(x){
  x <- gsub(' ', '', x)
  m <- gregexpr('\\(인\\)',  x)
  tmp <- unlist(lapply(regmatches(df$ru_name, m), function(x) paste(sort(unique(x)), collapse=' ')))
  tmp[tmp==""] <- NA
  return(tmp)
}

## | 2.2 메타 변수 생성 ####
df <- df%>%
  mutate(cell_name = rmMeta(ru_name))%>%
  mutate(meta_parenthesis = getParenthesis(ru_name))%>%
  mutate(meta_bracket = getBracket(ru_name))%>%
  mutate(meta_train = getTrain(ru_name))%>%
  mutate(meta_apt = getApt(ru_name))%>%
  mutate(meta_prkg = getParkinglot(ru_name))%>%
  mutate(meta_inbldg = getInbuilding(ru_name))


## 3. hive에 저장
## 3.1 스파크 메모리에 올리기 ####
sdf_copy_to(sc, df, 'sp_cell_meta', overwrite = TRUE)

## 3.2 hive table로 저장하기
#dbSendQuery(sc, "use 데이터베이스변수")
dbSendQuery(sc, paste( "use",args[1],collapse=" "))
dbSendQuery(sc, "drop table lpmgt_cell_meta purge")
dbSendQuery(sc, "create table if not exists lpmgt_cell_meta as select * from sp_cell_meta")


spark_disconnect(sc)
