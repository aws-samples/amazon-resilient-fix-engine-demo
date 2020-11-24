CREATE DATABASE quickfix;

USE quickfix;

CREATE USER fixengine IDENTIFIED BY 'password';

CREATE TABLE sessions (
  beginstring CHAR(8) NOT NULL,
  sendercompid VARCHAR(64) NOT NULL,
  targetcompid VARCHAR(64) NOT NULL,
  session_qualifier VARCHAR(64) NOT NULL,
  creation_time DATETIME NOT NULL,
  incoming_seqnum INT NOT NULL, 
  outgoing_seqnum INT NOT NULL,
  PRIMARY KEY (beginstring, sendercompid, targetcompid, session_qualifier)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.sessions TO fixengine;

CREATE TABLE messages (
  beginstring CHAR(8) NOT NULL,
  sendercompid VARCHAR(64) NOT NULL,
  targetcompid VARCHAR(64) NOT NULL,
  session_qualifier VARCHAR(64) NOT NULL,
  msgseqnum INT NOT NULL, 
  message BLOB NOT NULL,
  PRIMARY KEY (beginstring, sendercompid, targetcompid, session_qualifier, msgseqnum)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.messages TO fixengine;

CREATE TABLE messages_log (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  time DATETIME NOT NULL,
  time_milliseconds INT NOT NULL,
  beginstring CHAR(8),
  sendercompid VARCHAR(64),
  targetcompid VARCHAR(64),
  session_qualifier VARCHAR(64),
  text BLOB NOT NULL,
  PRIMARY KEY (id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.messages_log TO fixengine;

CREATE TABLE messages_backup_log (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  time DATETIME NOT NULL,
  time_milliseconds INT NOT NULL,
  beginstring CHAR(8),
  sendercompid VARCHAR(64),
  targetcompid VARCHAR(64),
  session_qualifier VARCHAR(64),
  text BLOB NOT NULL,
  PRIMARY KEY (id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.messages_backup_log TO fixengine;

CREATE TABLE event_log (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  time DATETIME NOT NULL,
  time_milliseconds INT NOT NULL,
  beginstring CHAR(8),
  sendercompid VARCHAR(64),
  targetcompid VARCHAR(64),
  session_qualifier VARCHAR(64),
  text BLOB NOT NULL,
  PRIMARY KEY (id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.event_log TO fixengine;

CREATE TABLE event_backup_log (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  time DATETIME NOT NULL,
  time_milliseconds INT NOT NULL,
  beginstring CHAR(8),
  sendercompid VARCHAR(64),
  targetcompid VARCHAR(64),
  session_qualifier VARCHAR(64),
  text BLOB NOT NULL,
  PRIMARY KEY (id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON quickfix.event_backup_log TO fixengine;

CREATE TABLE ActiveEngine  (
  TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ipAddress VARCHAR(16) NOT NULL,
  PRIMARY KEY (TS, ipAddress)
);
GRANT SELECT, UPDATE ON quickfix.ActiveEngine TO fixengine;

INSERT INTO ActiveEngine VALUES (NOW(), "none");


DELIMITER //

create procedure EngineStatus(IN ipAdd varchar(16), OUT value int, OUT lastIpAdd varchar(16), OUT lastTimestamp DATETIME, OUT timeNow DATETIME, OUT timeDiffSec int)
BEGIN
	START TRANSACTION;
    SET timeNow = NOW();
    SELECT TS, timestampdiff (second, TS, timeNow), ipAddress INTO lastTimestamp, timeDiffSec, lastIpAdd FROM ActiveEngine;
    IF timeDiffSec <= 3 AND ipAdd != lastIpAdd THEN            # If another IP is the leader and not stale 
      SET value = 0;                                           # Then I do nothing and keep waiting
    ELSE 
      UPDATE ActiveEngine SET TS = timeNow, ipAddress = ipAdd; # Else I am going to be the leader (update DB to say this) 
      IF (timeDiffSec > 3) THEN                                #   If the last leader is stale (no matter if it was me or another IP)
        SET value = -1;                                        #   Then I need to become the new leader
      ELSE
        SET value = 1;                                         #   Else I am still the active leader
      END IF;
    END IF;
    COMMIT;
END //

DELIMITER ;

GRANT EXECUTE ON PROCEDURE quickfix.EngineStatus TO fixengine;

GRANT SELECT ON mysql.proc TO fixengine;

