'use strict';

const crypto = require('crypto');
const randomBytes = crypto.randomBytes;

const digits = '0123456789';
const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const charset = [digits, letters, letters.toLowerCase()].join('');

exports.generateId = (type) => {
    return crypto.randomBytes(16).map(
        value => charset.charCodeAt(
            Math.floor(value * charset.length / 256)
        )
    ).toString();
}

exports.inputRecord = function (type, record) {

    const recordTypes = this.recordTypes;
    const options = this.options;
    const typeMap = options.typeMap || {};
    const primaryKey = this.keys.primary;
    const isArrayKey = this.keys.isArray;
    const fields = recordTypes[type];
    const generateId = 'generateId' in options ? options.generateId : this.generateId;

    const typeIdColumns = this._computeTypeIdColumns(type);
    const typeId = typeIdColumns[typeIdColumns.length - 1];

    if (!(primaryKey in record) && generateId) {
        record[primaryKey] = generateId(type);
    }

    if (primaryKey in record) {
        record[typeId] = record[primaryKey];
        delete record[primaryKey];
    }

    return record;

}

exports.outputRecord = function (type, record) {

    const clone = {};
    const recordTypes = this.recordTypes;

    const primaryKey = this.keys.primary;
    const isArrayKey = this.keys.isArray;
    const typeKey = this.keys.type;
    const denormalizedInverseKey = this.keys.denormalizedInverse;
    const fields = recordTypes[type];

    const typeIdColumns = this._computeTypeIdColumns(type);
    const typeId = typeIdColumns[typeIdColumns.length - 1];

    clone[primaryKey] = record[typeId];

    for (const field in fields) {

        const fieldType = fields[field][typeKey];
        const fieldIsArray = fields[field][isArrayKey];
        const value = record[field];

        if (fields[field][denormalizedInverseKey]) {
            Object.defineProperty(clone, field, {
                value,
                writable: true,
                configurable: true
            });
            continue;
        }

        if (field in record) {
            clone[field] = value;
        }

    }

    return clone;

}