CREATE NODE TABLE Comment(ID INT64, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, content STRING, length INT64, PRIMARY KEY(ID));
CREATE NODE TABLE Person(ID INT64, firstName STRING, lastName STRING, gender STRING, birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, PRIMARY KEY(ID));
CREATE NODE TABLE Place(ID INT64, name STRING, url STRING, type STRING, PRIMARY KEY(ID));
CREATE REL TABLE personIsLocatedIn(FROM Person TO Place, MANY_ONE);
CREATE REL TABLE likeComment(FROM Person TO Comment, creationDate TIMESTAMP, MANY_MANY);
