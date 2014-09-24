{ Promise } = require 'es6-promise'
fs = require 'fs'
async = require 'async'

logPrefix = 'migrator'

module.exports = migrator =
	logger: console
	database: null
	directory: './migrations'
	tableName: 'schema_migrations'

	migrate: (db) ->
		createMigrationTable = ->
			new Promise (resolve, reject) ->
				db.query 'select count(*) as cnt from information_schema.tables where lower(table_schema) = lower(?) and lower(table_name) = lower(?)', [ migrator.database, migrator.tableName ], (err, rows) ->
					return reject err if err
					return resolve() if rows[0].cnt
					migrator.logger.info '[%s] creating table %s', logPrefix, migrator.tableName
					db.query 'create table #{migrator.tableName} (migration_id varchar(255) not null primary key, executed_at timestamp not null) engine=MyISAM', (err) ->
						return reject err if err
						resolve()

		collectMigrations = ->
			new Promise (resolve, reject) ->
				migrator.logger.debug '[%s] loading migrations from directory %s', logPrefix, migrator.directory
				fs.readdir migrator.directory, (err, files) ->
					return reject err if err
					files = files.filter (f) -> f.match /\.sql$/
					files = files.sort()
					resolve files

		selectMigrations = (migrationIds) ->
			isApplied = (migrationId) ->
				new Promise (resolve, reject) ->
					db.query 'select count(*) cnt from #{migrator.tableName} where migration_id = ?', [ migrationId ], (err, rows) ->
						return reject err if err
						resolve rows[0].cnt is 1

			Promise.all migrationIds.map isApplied
			.then (applied) ->
				migrationIds.filter (f, i) -> not applied[i]

		executeMigrations = (migrationIds) ->
			executeMigration = (migrationId) ->
				(next) ->
					migrator.logger.info '[%s] executing migration %s', logPrefix, migrationId
					fs.readFile "#{migrator.directory}/#{migrationId}", { encoding: 'utf8' }, (err, content) ->
						return next err if err
						tasks = content.split(/;/).filter((s) -> s.trim()).map (statement) -> (next) ->
							db.query statement, next
						async.series tasks, (err) ->
							return next err if err
							db.query 'insert into #{migrator.tableName} set ?', { migration_id: migrationId, executed_at: new Date }, next

			new Promise (resolve, reject) ->
				tasks = migrationIds.map executeMigration
				async.series tasks, (err) ->
					return reject err if err
					resolve migrationIds

		migrator.logger.info '[%s] migrating database %s...', logPrefix, migrator.database
		createMigrationTable()
		.then collectMigrations
		.then selectMigrations
		.then executeMigrations
		.then -> migrator.logger.info '[%s] migration complete.', logPrefix
