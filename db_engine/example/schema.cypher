CREATE NODE TABLE A(ID INT64, content STRING, PRIMARY KEY(ID));
CREATE NODE TABLE B(ID INT64, content STRING, PRIMARY KEY(ID));
CREATE NODE TABLE C(ID INT64, content STRING, PRIMARY KEY(ID));
CREATE REL TABLE A_B(FROM A TO B, MANY_MANY);
CREATE REL TABLE B_C(FROM B TO C, MANY_MANY);