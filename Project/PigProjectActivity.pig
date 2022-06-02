inputFile = LOAD 'hdfs:///user/ProjectActivity1/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray,dial:chararray);
--ranked = RANK inputFile;
--onlydia = FILTER ranked BY (rank_inputFile >2);
GroupByName = GROUP inputFile BY name;
-- Generate result format
CountDia = FOREACH GroupByName GENERATE $0 as name, COUNT($1) AS CountVal;
name_order = ORDER CountDia BY CountVal DESC;
-- Store the result in HDFS
STORE name_order INTO 'hdfs:///user/ProjectActivity1/episodeIVResult' USING PigStorage('\t');


inputFile = LOAD 'hdfs:///user/ProjectActivity1/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray,dial:chararray);
--ranked = RANK inputFile;
--onlydia = FILTER ranked BY (rank_inputFile >2);
GroupByName = GROUP inputFile BY name;
-- Generate result format
CountDia = FOREACH GroupByName GENERATE $0 as name, COUNT($1) AS CountVal;
name_order = ORDER CountDia BY CountVal DESC;
-- Store the result in HDFS
STORE name_order INTO 'hdfs:///user/ProjectActivity1/episodeVResult' USING PigStorage('\t');




inputFile = LOAD 'hdfs:///user/ProjectActivity1/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray,dial:chararray);
--ranked = RANK inputFile;
--onlydia = FILTER ranked BY (rank_inputFile >2);
GroupByName = GROUP inputFile BY name;
-- Generate result format
CountDia = FOREACH GroupByName GENERATE $0 as name, COUNT($1) AS CountVal;
name_order = ORDER CountDia BY CountVal DESC;
-- Store the result in HDFS
STORE name_order INTO 'hdfs:///user/ProjectActivity1/episodeVIResult' USING PigStorage('\t');