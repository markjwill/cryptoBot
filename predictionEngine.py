#!/usr/bin/python

import mariadb

import priceAverageGroup
import predictionTarget

import credentials

class predictionEngine:

    name = ''

    trades = []

    priceAverageGroups = []

    predictionTargets = []

    def __init__(self, name, priceAverageGroupsList, predictionTargetsList):

        self.name = name

        try:
            self.conn = mariadb.connect(
                user=credentials.dbUser,
                password=credentials.dbPassword,
                host=credentials.dbHost,
                port=credentials.dbPort,
                database=credentials.dbName
            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.cur = self.conn.cursor()

        for priceAverageGroupData in priceAverageGroupsList:
            self.priceAverageGroups.append(priceAverageGroup.priceAverageGroup(priceAverageGroupData['name'], priceAverageGroupData['seconds']))

        for predictionTargetData in predictionTargetsList:
            self.predictionTargets.append(predictionTarget.predictionTarget(predictionTargetData['name'], predictionTargetData['seconds']))

        if not self.tableExists():
            self.createTable()


    def tableExists(self):
        self.cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(self.name.replace('\'', '\'\'')))
        if self.cur.fetchone()[0] == 1:
            return True

        return False

    def createTable(self):
        columns = ''
        averageGroupsIndex = None
        predictionTargetsIndex = None
        for priceGroup in self.priceAverageGroups:
            for param in priceGroup.params.keys():
                name = priceGroup.name+"_"+param
                columns += "`{0}` float DEFAULT NULL, ".format(name)
                if averageGroupsIndex is None:
                    averageGroupsIndex = name
        for target in self.predictionTargets:
            for param in target.params.keys():
                name = target.name+"_"+param
                columns += "`{0}` float DEFAULT NULL, ".format(name)
                if predictionTargetsIndex is None:
                    predictionTargetsIndex = name

        self.cur.execute("""
            CREATE TABLE `{0}` (
            `id` bigint(20) NOT NULL,
            `amount` float DEFAULT NULL,
            `date` int(11) DEFAULT NULL,
            `date_ms` bigint(20) DEFAULT NULL,
            `price` float DEFAULT NULL,
            `type` varchar(4) DEFAULT NULL,
            `market` varchar(7) DEFAULT NULL,
            `updated` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
            {1}
            PRIMARY KEY (`id`),
            KEY `date` (`date`) USING BTREE,
            KEY `date_ms` (`date_ms`) USING BTREE,
            KEY `{2}` (`{2}`) USING BTREE,
            KEY `{3}` (`{3}`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """.format(self.name, columns, averageGroupsIndex, predictionTargetsIndex))


