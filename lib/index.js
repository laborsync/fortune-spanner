'use strict'

const Spanner = require('@google-cloud/spanner');

const helpers = require('./helpers');

const generateId = helpers.generateId;
const inputRecord = helpers.inputRecord;
const outputRecord = helpers.outputRecord;

const adapterOptions = new Set(['generateId', 'typeMap']);

const logicalOperators = ['and', 'or'];

const findRecordsKey = Symbol('findRecords');

module.exports = Adapter => class SpannerAdapter extends Adapter {

    connect() {

        const Promise = this.Promise;
        const options = this.options || {};

        if (!('generateId' in options)) options.generateId = generateId;
        if (!('typeMap' in options)) options.typeMap = {};

        if (!this.client) {

            const databaseId = options.databaseId;
            const projectId = options.projectId;
            const instanceId = options.instanceId;
            const keepAlive = options.keepAlive;

            const spannerOptions = {
                projectId
            };

            this.spanner = Spanner(spannerOptions);

            const instanceOptions = {};
            if (keepAlive) {
                instanceOptions.keepAlive = keepAlive;
            }

            this.instance = this.spanner.instance(instanceId, instanceOptions);

            this.database = this.instance.database(databaseId);

            this.client = this.database;

        }

        return Promise.resolve();

    }

    disconnect() {

        const client = this.client;

        return this.database.close()
            .then(() => {
                client = null;
            });

    }

    _computeSelect(type, options) {

        options = options || {};
        options.fields = options.fields || {}

        const recordTypes = this.recordTypes;
        const denormalizedInverseKey = this.keys.denormalizedInverse

        let select = [];

        const fields = Object.keys(options.fields);
        if (fields.length) {
            select = fields.filter((field) => {
                return !denormalizedInverseKey in recordTypes[type][field] && recordTypes[type][field];
            });
        }

        if (select.length) {
            this._computeTypeIdColumns(type).forEach((column) => {
                if (!options.fields[column]) {
                    select.push(column);
                }
            });
        }

        return 'SELECT ' + (select.length ? select.join(', ') : '*');

    }

    _computeTypeIdColumns(type) {

        const typeMap = this.options.typeMap || {};

        if (!typeMap[type].id === undefined) {
            throw new Error('A typeMap id is required for type "' + type + '"');
        }

        return Array.isArray(typeMap[type].id) ? typeMap[type].id : [typeMap[type].id];

    }

    _computeTable(type) {

        const typeMap = this.options.typeMap || {};
        if (typeof typeMap[type].table !== 'string') {
            throw new Error('A typeMap table is required for type "' + type + '"');
        }

        return typeMap[type].table;

    }

    _computeTypeNumberKeys(type) {

        const recordTypes = this.recordTypes;

        return Object.keys(recordTypes[type]).filter((key) => {
            return recordTypes[type][key].type === Number;
        });

    }


    _computeTableIndexColumns(type, ids, options, columns) {

        options = options || {};
        columns = columns || {};

        if (ids) {
            const typeIdColumns = this._computeTypeIdColumns(type);
            const typeId = typeIdColumns[typeIdColumns.length - 1];
            columns[typeId] = true;
        }

        for (const option in options) {

            if (logicalOperators.indexOf(option) >= 0) {
                columns = this._computeTableIndexColumns(type, null, options[option], columns)
                continue;
            }

            for (const column in options[option]) {

                if (logicalOperators.indexOf(column) >= 0) {
                    columns = this._computeTableIndexColumns(type, null, options[option][column], columns)
                    continue;
                }

                columns[column] = true;

            }

        }

        return columns;

    }



    _computeTableIndexRanks(type, ids, options) {

        const typeMap = this.options.typeMap || {};
        const typeIndexes = typeMap[type].indexes || {};
        const primaryIndex = this._computeTable(type);
        typeIndexes[primaryIndex] = this._computeTypeIdColumns(type);

        const columns = this._computeTableIndexColumns(type, ids, options);

        let index = primaryIndex;
        const ranks = {};

        for (const typeIndex in typeIndexes) {

            ranks[typeIndex] = typeIndexes[typeIndex].filter((column) => {
                return columns[column];
            }).length;

        }

        return ranks;

    }

    _computeTableIndex(type, ids, options) {

        const typeMap = this.options.typeMap || {};
        const typeIndexes = typeMap[type].indexes || {};
        const primaryIndex = this._computeTable(type);
        typeIndexes[primaryIndex] = this._computeTypeIdColumns(type);

        const tableIndexRanks = this._computeTableIndexRanks(type, ids, options);

        let tableIndex = primaryIndex;
        for (const tableIndexRank in tableIndexRanks) {
            if (tableIndexRanks[tableIndexRank] >= tableIndexRanks[tableIndex] && typeIndexes[tableIndexRank].length <= typeIndexes[tableIndex].length) {
                tableIndex = tableIndexRank;
            }
        }

        return tableIndex != primaryIndex ? '@{FORCE_INDEX=' + tableIndex + '}' : '';

    }

    _computeFrom(type, ids, options) {
        return 'FROM ' + this._computeTable(type, options) + this._computeTableIndex(type, ids, options);
    }

    _computeWhere(type, ids, options, meta, params, root = true) {

        const where = this._computeWhereLogicalOperators(type, options, meta, params, [
            this._computeWhereIds(type, ids, params),
            this._computeWhereExists(type, options, meta, params),
            this._computeWhereMatch(type, options, meta, params),
            this._computeWhereRange(type, options, meta, params),
        ].filter(Boolean).join(' AND '));

        return where ? (root ? 'WHERE ' + where : '(' + where + ')') : null;

    }

    _computeSort(type, options) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const sort = [];
        for (const field in options.sort) {

            const isArray = recordTypes[type][field] && recordTypes[type][field][isArrayKey];
            const order = options.sort[field] ? 'ASC' : 'DESC';

            if (!isArray) {
                sort.push(field + ' ' + order);
            } else {
                sort.push('COALESCE(ARRAY_LENGTH(' + field + ', 1), 0) ' + order);
            }

        }

        return sort.length ? ('ORDER BY ' + sort.join(', ')) : null;

    }

    _computeLimit(options) {
        return options.limit ? 'LIMIT ' + options.limit : null;
    }

    _computeOffset(options) {
        return options.offset ? 'OFFSET ' + options.limit : null;
    }

    _computeWhereIds(type, ids, params) {

        const typeIdColumns = this._computeTypeIdColumns(type);
        const typeId = typeIdColumns[typeIdColumns.length - 1];

        const whereIds = [];

        if (ids) {

            if (!Array.isArray(ids)) {
                ids = [ids];
            }

            ids.map((id) => {
                const param = 'id' + Object.keys(params).length;
                params[param] = id;
                whereIds.push('@' + param);
            });

        }

        return whereIds.length ? typeId + ' IN (' + whereIds.join(', ') + ')' : null;

    }

    _computeWhereExists(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereExists = [];

        for (const field in options.exists) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            const isArray = recordTypes[type][field] && recordTypes[type][field][isArrayKey];
            const value = options.exists[field];

            if (!isArray) {
                whereExists.push(field + ' ' + (value ? 'IS NOT NULL' : 'IS NULL'));
                continue;
            }

            whereExists.push('COALESCE(ARRAY_LENGTH(' + field + ', 1), 0) ' + (value ? '> 0' : '= 0'));

        }

        return whereExists.length ? this._computeWhereLogicalOperators(type, options.range, meta, params, whereExists.join(' AND ')) : null;

    }

    _computeWhereMatch(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereMatch = [];

        for (const field in options.match) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            const isArray = recordTypes[type][field] && recordTypes[type][field][isArrayKey];
            let values = options.match[field];

            if (!Array.isArray(values)) {
                values = [values];
            }

            const whereOr = [];

            if (!isArray) {

                const whereIn = [];

                values.map((value) => {
                    const param = 'match' + Object.keys(params).length;
                    params[param] = value;
                    whereIn.push('@' + param);
                });

                whereOr.push(field + ' IN (' + whereIn.join(', ') + ')');

            } else {

                if (!Array.isArray(values[0])) {
                    values = [values];
                }

                values.map((value) => {

                    const whereIn = [];

                    value.map((match) => {
                        const param = 'match' + Object.keys(params).length;
                        params[param] = match;
                        whereIn.push('@' + param + ' IN UNNEST(' + field + ')');
                    });

                    whereOr.push('(' + whereIn.join(' AND ') + ')');

                });

            }

            whereMatch.push('(' + whereOr.join(' OR ') + ')');

        }

        return whereMatch.length ? this._computeWhereLogicalOperators(type, options.match, meta, params, whereMatch.join(' AND ')) : null;

    }

    _computeTypeRequestNowKeys(type, request) {

        const typeMap = this.options.typeMap || {};

        if (!typeMap[type]) {
            throw new Error('A typeMap is required for type "' + type + '".');
        }

        const nowKeys = [];

        if (request === 'create') {
            if (typeMap[type].created) {
                nowKeys.push(typeMap[type].created);
            }
        }

        if (typeMap[type].updated) {
            nowKeys.push(typeMap[type].updated);
        }

        return nowKeys;

    }

    _computeWhereRange(type, options, meta, params) {

        const recordTypes = this.recordTypes;
        const isArrayKey = this.keys.isArray;

        const whereRange = [];

        for (const field in options.range) {

            if (logicalOperators.indexOf(field) >= 0) {
                continue;
            }

            const isArray = recordTypes[type][field] && recordTypes[type][field][isArrayKey];
            let values = options.range[field];

            if (!Array.isArray(values[0])) {
                values = [values];
            }

            const whereOr = [];

            values.map((value) => {

                const whereAnd = [];

                value.map((range, index) => {

                    if (range != null) {

                        const param = 'range' + Object.keys(params).length;
                        params[param] = range;

                        if (!isArray) {
                            whereAnd.push(field + ' ' + (index ? '<' : '>') + '= @' + param);
                        } else {
                            whereAnd.push('COALESCE(ARRAY_LENGTH(' + field + ', 1), 0)  ' + (index ? '<' : '>') + '= @' + param);
                        }

                    }

                });

                whereOr.push('(' + whereAnd.join(' AND ') + ')');

            });

            whereRange.push('(' + whereOr.join(' OR ') + ')');

        }

        return whereRange.length ? this._computeWhereLogicalOperators(type, options.range, meta, params, whereRange.join(' AND ')) : null;

    }

    _computeWhereLogicalOperators(type, options, meta, params, where) {

        if (!where.length) {
            return null;
        }

        logicalOperators.map((logicalOperator) => {

            if (options[logicalOperator]) {
                where = [
                    where,
                    this._computeWhere(type, undefined, options[logicalOperator], meta, params, false)
                ].filter(Boolean);

                where = '(' + where.join(' ' + logicalOperator.toUpperCase() + ' ') + ')';
            }

        });

        return where;

    }

    create(type, records, meta) {

        if (!records.length) {
            return super.create();
        }

        const Promise = this.Promise;
        const client = this.client;

        return new Promise((resolve, reject) => {

            const nowKeys = this._computeTypeRequestNowKeys(type, 'create');
            const promises = nowKeys.length ? [this._queryNow()] : [];

            return Promise.all(promises)
                .then(([now]) => {

                    const typeCasts = this._computeTypeCasts(type);

                    records = records.map((record) => {

                        record = inputRecord.call(this, type, record);

                        nowKeys.map((nowKey) => {
                            record[nowKey] = now;
                        });

                        typeCasts.forEach((typeCast) => {
                            record[typeCast.field] = typeCast.cast(record[typeCast.field]);
                        });

                        return record;

                    });

                    client.insert(this._computeTable(type), records);

                    return records.map(outputRecord.bind(this, type));

                })
                .then(resolve)
                .catch(reject);

        });

    }

    delete(type, ids, meta) {

        // Handle no-op
        if (ids && !ids.length) {
            return super.delete();
        }

        const Promise = this.Promise;
        const client = this.client;

        const typeIdColumns = this._computeTypeIdColumns(type);

        const deleteKeys = meta[findRecordsKey][type].map((record) => {
            return typeIdColumns.map((typeIdColumn) => {
                return record[typeIdColumn];
            });
        });

        return new Promise((resolve, reject) => {
            client.deleteRows(this._computeTable(type), deleteKeys);
            return resolve(deleteKeys.length);
        });

    }

    update(type, updates, meta) {

        // Handle no-op
        if (!updates.length) {
            return super.update();
        }

        const Promise = this.Promise;
        const client = this.client;

        const typeMap = this.options.typeMap || {};
        if (!typeMap[type]) {
            throw new Error('A typeMap is required for type "' + type + '".');
        }

        const primaryKey = this.keys.primary;

        return new Promise((resolve, reject) => {

            const nowKeys = this._computeTypeRequestNowKeys(type, 'update');
            const promises = nowKeys.length ? [this._queryNow()] : [];

            return Promise.all(promises)
                .then(([now]) => {

                    const spanner = this.spanner;
                    const typeIdColumns = this._computeTypeIdColumns(type);
                    const typeId = typeIdColumns[typeIdColumns.length - 1];

                    const recordsMap = {};
                    const recordsChanged = {};

                    const typeCasts = this._computeTypeCasts(type);

                    let records = meta[findRecordsKey][type] || [];
                    records = records.map((record, index) => {

                        recordsMap[record[typeId]] = index;

                        nowKeys.map((nowKey) => {
                            record[nowKey] = now;
                        });

                        typeCasts.forEach((typeCast) => {
                            record[typeCast.field] = typeCast.cast(record[typeCast.field]);
                        })

                        return record;

                    });

                    updates.map((update) => {

                        if (update.operate !== undefined) {
                            throw new Error('Option operate for update not supported');
                        }

                        const recordIndex = recordsMap[update.id];

                        update.replace = update.replace || {};
                        Object.getOwnPropertyNames(update.replace).map((field) => {
                            records[recordIndex][field] = update.replace[field];
                            recordsChanged[update.id] = true;
                        });

                        update.push = update.push || {};
                        Object.getOwnPropertyNames(update.push).map((field) => {

                            if (typeMap[type][field] === undefined) {
                                return;
                            }

                            if (!Array.isArray(update.push)) {
                                update.push = [update.push];
                            }

                            records[recordIndex][field] = Array.isArray(records[recordIndex][field]) ? records[recordIndex][field] : [];

                            records[recordIndex][field] = records[recordIndex][field].concat(update.push);

                            records[recordIndex][field].filter(function (record, index, self) {
                                return index == self.indexOf(record);
                            });

                            records[recordIndex][field] = records[recordIndex][field].length ? records[recordIndex][field] : null;
                            recordsChanged[update.id] = true;

                        });

                        update.pull = update.pull || {};
                        Object.getOwnPropertyNames(update.pull).map((field) => {

                            if (typeMap[type][field] === undefined) {
                                return;
                            }

                            if (!Array.isArray(update.pull)) {
                                update.pull = [update.pull];
                            }

                            records[recordIndex][field] = Array.isArray(records[recordIndex][field]) ? records[recordIndex][field] : [];

                            records[recordIndex][field] = records[recordIndex][field].filter((record) => {
                                return update.pull.indexOf(record) < 0
                            });

                            records[recordIndex][field] = records[recordIndex][field].length ? records[recordIndex][field] : null;
                            recordsChanged[update.id] = true;

                        });

                    });

                    const recordChanges = records.filter((record) => {
                        return recordsChanged[record[typeId]];
                    });

                    if (recordChanges.length) {
                        client.update(this._computeTable(type), recordChanges);
                    }

                    return records.map(outputRecord.bind(this, type));

                })
                .then(resolve)
                .catch(reject);

        });

    }

    find(type, ids, options, meta) {

        // Handle no-op
        if (ids && !ids.length) {
            return super.find();
        }

        options = options || {};

        if (options.query) {
            throw new Error('Option query for find not supported');
        }

        meta = meta || {};

        const Promise = this.Promise;
        const client = this.client;
        const isArrayKey = this.keys.isArray;

        const typeMap = this.options.typeMap || {};
        if (!typeMap[type]) {
            throw new Error('A typeMap is required for type "' + type + '".');
        }

        const selectSql = this._computeSelect(type, options);
        const fromSql = this._computeFrom(type, ids, options);

        const params = {};
        const whereSql = this._computeWhere(type, ids, options, meta, params);

        const sortSql = this._computeSort(type, options);
        const limitSql = this._computeLimit(options);
        const offsetSql = this._computeOffset(options);

        const findQuery = {
            sql: [
                selectSql,
                fromSql,
                whereSql,
                sortSql,
                limitSql,
                offsetSql
            ].filter(Boolean).join(' '),
            params: params
        }

        const countQuery = {
            sql: [
                'SELECT COUNT(*) as count',
                fromSql,
                whereSql
            ].filter(Boolean).join(' '),
            params: params
        }

        meta[findRecordsKey] = meta[findRecordsKey] || {};
        meta[findRecordsKey][type] = meta[findRecordsKey][type] || [];

        const hrstart = process.hrtime();

        return Promise.all([
                client.run(findQuery),
                client.run(countQuery)
            ])
            .then(([
                [findResults],
                [countResults]
            ]) => {

                const hrend = process.hrtime(hrstart);

                let duration = hrend[0] ? hrend[0] + 's ' : '';
                duration += (hrend[1] / 1000000).toFixed(3) + 'ms';

                console.log({
                    sql: findQuery.sql,
                    duration
                });

                const numberKeys = this._computeTypeNumberKeys(type);

                const typeCasts = typeMap[type].typeCasts || {};
                ['created', 'updated'].forEach((field) => {
                    if (typeMap[type][field]) {
                        typeCasts[field] = 'timestamp';
                    }
                });

                const dateCasts = {
                    'date': true,
                    'timestamp': true
                }

                const dateKeys = Object.keys(typeCasts).filter((field) => {
                    return Boolean(dateCasts[typeCasts[field]]);
                });

                const records = findResults.map((findResult, index) => {

                    const record = findResult.toJSON();

                    numberKeys.map((numberKey) => {
                        record[numberKey] = Number(record[numberKey].value);
                    });

                    dateKeys.map((dateKey) => {
                        record[dateKey] = record[dateKey].toJSON();
                    });

                    meta[findRecordsKey][type].push(record);

                    return outputRecord.call(this, type, record);

                });

                records.count = Number(countResults[0].toJSON().count.value);

                return records;

            })

    }

    beginTransaction() {

        const client = this.client;

        return client.getTransaction()
            .then(([transaction]) => {

                const scope = Object.create(Object.getPrototypeOf(this))

                Object.assign(scope, this, {
                    client: transaction,
                    endTransaction(error) {
                        return new Promise((resolve, reject) => {
                            return transaction[error ? 'end' : 'commit']()
                                .then(resolve)
                                .catch(reject);
                        });
                    }
                })

                return scope;

            });

    }

    _queryNow() {

        const client = this.client;

        const nowQuery = {
            sql: 'SELECT CURRENT_TIMESTAMP() AS now'
        }

        return client.run(nowQuery)
            .then(([nowResults]) => {
                return nowResults[0].toJSON().now;
            });

    }

    _computeTypeCastInt(value) {
        const spanner = this.spanner;
        return value != null ? spanner.int(value) : value;
    }

    _computeTypeCastFloat(value) {
        const spanner = this.spanner;
        return value != null ? spanner.float(value) : value;
    }

    _computeTypeCastDate(value) {
        return value != null ? this._computeTypeCastTimestamp(value).replace(/T.+/, '') : value;
    }

    _computeTypeCastTimestamp(value) {
        return value != null ? new Date(value).toJSON() : value;
    }

    _computeTypeCasts(type) {

        const typeMap = this.options.typeMap || {};
        const typeCasts = typeMap[type].typeCasts || {};

        const casts = {
            'int': this._computeTypeCastInt,
            'float': this._computeTypeCastFloat,
            'date': this._computeTypeCastDate,
            'timestamp': this._computeTypeCastTimestamp
        };

        return Object.keys(typeCasts).map((field) => {
            return {
                field: field,
                cast: casts[typeCasts[field]].bind(this)
            }
        });

    }
}