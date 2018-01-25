# # 이름 매칭을 위한 기지국 메타 테이블 생성
# * 요약
# : 기지국-빌딩 매핑 테이블에 기지국 메타 테이블이 2길이 단어 array를 포함한 메타 추가

## 스파크 연결 ####
library(sparklyr)
library(DBI)

# Arguments
args = commandArgs(trailingOnly=TRUE)

# SET SPARK HOME
Sys.setenv(SPARK_HOME = "/usr/hdp/current/spark-client")
## configuration
conf <- spark_config()
conf[["spark.executor.instances"]] <- 4
conf[["spark.executor.cores"]] <- 8
conf[["spark.executor.memory"]] <- '8G'
conf[["spark.yarn.am.memory"]] <- '4G'
conf[["spark.yarn.am.cores"]] <- 2
# 연결
sc <- spark_connect(master = "yarn-client", config=conf)

# ## hive 연결 ####
# # configuration
# options( java.parameters = "-Xmx64g" )
# library(rJava)
# library(RJDBC)
# cp = c("/usr/hdp/current/hive-client/lib/hive-jdbc.jar",
#        "/usr/hdp/current/hadoop-client/hadoop-common.jar")
# .jinit(classpath=cp)
# drv <- JDBC("org.apache.hive.jdbc.HiveDriver",
#             "/usr/hdp/current/hive-client/lib/hive-jdbc.jar",
#             identifier.quote="`")
# # 연결
# conn <- dbConnect(drv, "jdbc:hive2://10.40.89.94:10000", "dbpuser4", "dbpuser4")

# 1. 위치 기준 join(기지국/twifi id 부여) ####
dbSendQuery(sc, paste( "use",args[1],collapse=" "))
dbSendQuery(sc, "drop table lpwip_cell_wifi_build_id purge")
dbSendQuery(sc, 'create table if not exists lpwip_cell_wifi_build_id stored as orc as
select distinct
  A.id
  ,A.lat
  ,A.lon
  ,A.type
  ,A.join_key
  ,A.used
  ,A.name1
  ,A.name2
  ,A.distanceki
  ,B.c_uid
  ,B.sector_id
    from
    (select *
      ,concat(lat, lon, type) as latlontype
      from
        src.cell_wifi_build A1) A
        ,(select *
            ,concat(latitude_nm, longitude_nm, type2) as latlontype
              from
              (select *
                ,case when (type="twifi") then "twifi" else "cell" end as type2
                from loc.lpmgt_cell_wifi_meta_temp_uniq) TMP) B
                where (A.latlontype = B.latlontype)')

# 2. 기지국 2-term 메타 계산/추가 ####
# 2. 1. ru_name에서 메타정보(괄호 안 키워드)를 제거한 이름 추출
#dbSendQuery(sc, 'use new_loc')

cell_name <- dbGetQuery(sc, 'select city_name, gu_name, dong_name, cell_name from lpmgt_cell_meta')
# 2.2. 2-길이 단어 생성 함수 생성
get2LengthTemrs <- function(x){
  #print(x$cell_name[1])
  ## 시/군/구 명 제거
  if(is.na(x$city_name[1])&is.na(x$gu_name[1])&is.na(x$dong_name[1])){
    locationterm <- NULL
  }else{
    locationterm <- unlist(lapply(as.list(1:(nchar(x$city_name[1])-1)), function(i, df) substr(df$city_name[1], i, i+1), x))
    locationterm <- c(locationterm, unlist(lapply(as.list(1:(nchar(x$gu_name[1])-1)), function(i, df) substr(df$gu_name[1], i, i+1), x)))
    locationterm <- c(locationterm, unlist(lapply(as.list(1:(nchar(x$dong_name[1])-1)), function(i, df) substr(df$dong_name[1], i, i+1), x)))
  }
  ## 기지국명 없거나 길이가 1인 경우 NA값 부여
  if(is.na(x$cell_name[1])){
    return(data.frame(cell_name= x$cell_name,
                      term = NA))
  }else if(nchar(x$cell_name[1])<=1){
    return(data.frame(cell_name= x$cell_name,
                      term=NA))
    ## 두 개 길이로 나누어 키워드 부여
    ## 특수문자 포함 키워드 제거
  }else{
    terms <- unlist(lapply(as.list(1:(nchar(x$cell_name[1])-1)), function(i, df) substr(df$cell_name[1], i, i+1), x))

    terms <- unlist(terms)
    terms <- terms[!terms%in%locationterm]
    terms <- terms[!grepl('[[:punct:]]',terms)]

    cell_name = rep(x$cell_name, length(terms))

    return(data.frame(cell_name = cell_name,
                      term=terms))
  }
}

# 2.3. 2-길이 단어 생성
library(dplyr)
library(plyr)
df.splited <- dlply(.data=cell_name, .variables='cell_name', .fun=get2LengthTemrs)
df.splited <- bind_rows(df.splited)
detach(package:plyr)
# 2.4. 스파크 테이블로 저장
sdf_copy_to(sc, df.splited, "sp_cell_meta2_2split")

# 3. 기지국 2-term 메타 계산/추가 hive테이블 생성 ####
#dbSendQuery(sc, "use new_loc")
dbSendQuery(sc, "drop table lpwip_cell_meta_2split purge")
dbSendQuery(sc, 'create table if not exists lpwip_cell_meta_2split stored as orc as
select
  A.c_uid
  ,A.cell_id as sector_id
  ,A.city_id
  ,A.city_id2 as gu_id
  ,A.dong_id
  ,A.city_name
  ,A.gu_name
  ,A.dong_name
  ,A.latitude
  ,A.longitude
  ,A.ru_name
  ,A.block_cd
  ,A.cell_name
  ,A.meta_parenthesis
  ,A.meta_bracket
  ,A.meta_train
  ,A.meta_apt
  ,A.meta_prkg
  ,A.meta_inbldg
  ,B.term as cell_termlist
  from
    lpmgt_cell_meta A
    left join
      (select
        cell_name
        ,collect_set(term) as term
          from sp_cell_meta2_2split
            where term is not null
          group by cell_name) B
      on (A.cell_name = B.cell_name)')

# 4. 기지국-빌딩 테이블에 메타 추가 ####
dbSendQuery(sc, 'drop table lpmgt_cell_build_meta purge')
dbSendQuery(sc, 'create table if not exists lpmgt_cell_build_meta stored as orc as
select
  A.id
  ,A.lat
  ,A.lon
  ,A.type
  ,A.join_key
  ,A.used
  ,A.name1
  ,A.name2
  ,A.distanceki
  ,A.c_uid
  ,A.sector_id
  ,B.city_id
  ,B.gu_id
  ,B.dong_id
  ,B.city_name
  ,B.gu_name
  ,B.dong_name
  ,B.latitude
  ,B.longitude
  ,B.ru_name
  ,B.block_cd
  ,B.cell_name
  ,B.meta_parenthesis
  ,B.meta_bracket
  ,B.meta_train
  ,B.meta_apt
  ,B.meta_prkg
  ,B.meta_inbldg
  ,B.cell_termlist
    from
      (select *
        ,concat(c_uid, "_",sector_id) as c_sector_id
          from
            lpwip_cell_wifi_build_id
            where type = "cell") as A
        left join
          (select *
            ,concat(c_uid, "_",sector_id) as c_sector_id
              from lpwip_cell_meta_2split) as B
              on (A.c_sector_id = B.c_sector_id)')

spark_disconnect(sc)
