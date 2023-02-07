USE KafkaKewl

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'kewl')
BEGIN
	EXEC('CREATE SCHEMA kewl AUTHORIZATION dbo')
END
GO

DROP PROCEDURE IF EXISTS kewl.StateChange_Insert
DROP PROCEDURE IF EXISTS kewl.StateChange_GetAll_Ascending
DROP PROCEDURE IF EXISTS kewl.StateChange_Wipe
DROP PROCEDURE IF EXISTS kewl.DeploymentStateChange_Insert
DROP PROCEDURE IF EXISTS kewl.DeploymentStateChange_GetAll_Ascending
DROP PROCEDURE IF EXISTS kewl.DeploymentStateChange_Wipe
DROP TABLE IF EXISTS kewl.StateChange
DROP TABLE IF EXISTS kewl.DeploymentStateChange
GO

CREATE TABLE kewl.StateChange
(
	StateChangeId	INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
	TransactionId   NVARCHAR(100) NOT NULL,
	StateChangeJson NVARCHAR(MAX) NOT NULL,
	TimeStampUtc	DATETIME DEFAULT(GETUTCDATE())
)
GO

CREATE TABLE kewl.DeploymentStateChange
(
	KafkaClusterId				NVARCHAR(50) NOT NULL,
	DeploymentStateChangeId		INT NOT NULL IDENTITY(1, 1),
	TransactionId				NVARCHAR(100) NOT NULL,
	DeploymentStateChangeJson	NVARCHAR(MAX) NOT NULL,
	TimeStampUtc				DATETIME DEFAULT(GETUTCDATE()),
	PRIMARY KEY (KafkaClusterId, DeploymentStateChangeId)
)
GO

CREATE PROCEDURE kewl.StateChange_Insert(
	@transactionId NVARCHAR(100),
	@stateChangeJson NVARCHAR(MAX)
)
AS
BEGIN
	INSERT INTO kewl.StateChange (TransactionId, StateChangeJson) 
	VALUES (@transactionId, @stateChangeJson)
END
GO

CREATE PROCEDURE kewl.StateChange_GetAll_Ascending
AS
BEGIN
	SELECT StateChangeId, TransactionId, StateChangeJson, TimeStampUtc
	FROM kewl.StateChange 
	ORDER BY StateChangeId
END
GO

CREATE PROCEDURE kewl.StateChange_Wipe
AS
BEGIN
	DELETE
	FROM kewl.StateChange 
END
GO

CREATE PROCEDURE kewl.DeploymentStateChange_Insert(
	@kafkaClusterId NVARCHAR(50),
	@transactionId NVARCHAR(100),
	@stateChangeJson NVARCHAR(MAX)
)
AS
BEGIN
	INSERT INTO kewl.DeploymentStateChange (KafkaClusterId, TransactionId, DeploymentStateChangeJson) 
	VALUES (@kafkaClusterId, @transactionId, @stateChangeJson)
END
GO

CREATE PROCEDURE kewl.DeploymentStateChange_GetAll_Ascending(
	@kafkaClusterId NVARCHAR(50)
)
AS
BEGIN
	SELECT KafkaClusterId, DeploymentStateChangeId, TransactionId, DeploymentStateChangeJson, TimeStampUtc
	FROM kewl.DeploymentStateChange
	WHERE (@kafkaClusterId IS NULL) OR (KafkaClusterId = @kafkaClusterId)
	ORDER BY DeploymentStateChangeId
END
GO

CREATE PROCEDURE kewl.DeploymentStateChange_Wipe(
	@kafkaClusterId NVARCHAR(50)
)
AS
BEGIN
	DELETE
	FROM kewl.DeploymentStateChange
	WHERE KafkaClusterId = @kafkaClusterId 
END
GO
