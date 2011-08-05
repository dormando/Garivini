CREATE TABLE `job` (
    `jobid`       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `funcname`    VARCHAR(200) NOT NULL,
    `arg`         MEDIUMBLOB,
    `uniqkey`     VARCHAR(255) NULL,
    `run_after`   INT UNSIGNED NOT NULL,
    `coalesce`    VARCHAR(255) NULL,
    PRIMARY KEY (jobid),
    INDEX (funcname, run_after),
    UNIQUE (funcname, uniqkey)
) ENGINE=InnoDB;
