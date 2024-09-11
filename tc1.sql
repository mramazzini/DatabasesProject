DROP TABLE IF EXISTS T1;
DROP TABLE IF EXISTS T2;
DROP TABLE IF EXISTS T3;

/*T1*/
CREATE TABLE T1 (k1 int, k2 int, A int, B int);

INSERT INTO T1 VALUES (1,10,1087,1087);
INSERT INTO T1 VALUES (2,20,1064,123);
INSERT INTO T1 VALUES (3,30,1031,123);
INSERT INTO T1 VALUES (4,40,1082,77);
INSERT INTO T1 VALUES (5,40,1031,654);


/*T2*/
CREATE TABLE T2 (k2 int, k3 int, C int);


INSERT INTO T2 VALUES (10,100,9999);
INSERT INTO T2 VALUES (20,300,9349);
INSERT INTO T2 VALUES (30,400,93299);
INSERT INTO T2 VALUES (40,200,99499);
INSERT INTO T2 VALUES (50,600,99);
INSERT INTO T2 VALUES (60,100,99199);


/*T3*/
CREATE TABLE T3 (k3 int, D int);


INSERT INTO T3 VALUES (100,88);
INSERT INTO T3 VALUES (200,888);
INSERT INTO T3 VALUES (300,8888);
INSERT INTO T3 VALUES (400,88);
INSERT INTO T3 VALUES (500,89);
INSERT INTO T3 VALUES (600,8888);
