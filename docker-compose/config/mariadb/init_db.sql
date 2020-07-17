CREATE USER 'maxuser'@'%' IDENTIFIED BY 'maxpwd';
GRANT REPLICATION SLAVE ON *.* TO 'maxuser'@'%';

DROP DATABASE IF EXISTS crm;
CREATE DATABASE IF NOT EXISTS crm;
USE crm;

DROP TABLE IF EXISTS contacts;

CREATE TABLE contacts (
    id          INT             NOT NULL    AUTO_INCREMENT,
    username    VARCHAR(120)    NOT NULL,
    name        VARCHAR(120)    NOT NULL,
    sex         ENUM ('M','F')  NOT NULL,
    address     VARCHAR(120)    NOT NULL,
    mail        VARCHAR(120)    NOT NULL,
    birthdate   DATE            NOT NULL,
    PRIMARY KEY (id)
);