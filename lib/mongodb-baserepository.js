/*
 * mongodb-baserepository
 * https://github.com/litixsoft/mongodb-baserepository
 *
 * Copyright (c) 2014 Litixsoft GmbH
 * Licensed under the MIT license.
 */

'use strict';

var lxHelpers = require('lx-helpers');

/**
 * Returns a base repo.
 * @param {!Object} collection The mongoDB collection.
 * @param {Object|Function=} schema
 * @returns {Object|Error} The repo object.
 * @constructor
 */
module.exports = function (collection, schema) {
    if (!lxHelpers.isObject(collection)) {
        throw lxHelpers.getTypeError('collection', collection, {});
    }

    schema = schema || {};

    var pub = {};
    var sort = {};
    var idFields = {};
    var key = '_id';
    var ObjectID = require('mongodb').ObjectID;

    /**
     * Analyses a property of the JSON schema for sorting, id fields, key or index.
     *
     * @param {!String} name The name of the property.
     * @param {!Object} prop The property object.
     */
    function analyseSchemaProperty (name, prop) {
        var idx = {};

        if (prop.type === 'string' && prop.format === 'mongo-id') {
            if (!idFields.hasOwnProperty(name)) {
                idFields[name] = true;
            }
        }

        if (prop.sort) {
            sort[name] = prop.sort;
        }

        if (prop.key) {
            key = name;
        }

        if (prop.hasOwnProperty('index')) {
            idx[name] = prop.index;

            collection.ensureIndex(idx, function (error) {
                if (error) {
                    console.log(error);
                }
            });
        }

        if (prop.hasOwnProperty('unique')) {
            idx[name] = prop.unique;

            collection.ensureIndex(idx, {unique: true}, function (error) {
                if (error) {
                    console.log(error);
                }
            });
        }
    }

    /**
     * Analyses the JSON schema to set sorting, keys, id-fields and indexes.
     *
     * @param {!Object} schema The JSON schema.
     * @param {String=} namespace The namespace for nested schemas.
     */
    function analyseSchema (schema, namespace) {
        namespace = namespace || '';

        lxHelpers.forEach(schema, function (propertyValue, propertyName) {
            propertyName = namespace === '' ? propertyName : namespace + '.' + propertyName;

            if (propertyValue.properties) {
                analyseSchema(propertyValue.properties, propertyName);
            } else if (propertyValue.items) {
                if (propertyValue.items.type && propertyValue.items.type !== 'array' && propertyValue.items.type !== 'object') {
                    analyseSchemaProperty(propertyName, propertyValue.items);
                } else if (propertyValue.items.type === 'object') {
                    analyseSchema(propertyValue.items.properties, propertyName);
                } else {
                    analyseSchema(propertyValue.items, propertyName);
                }
            } else {
                analyseSchemaProperty(propertyName, propertyValue);
            }
        });
    }

    function convertMongoIdsInArray (value) {
        var arrayQueryModifiers = ['$in', '$all', '$nin'];

        lxHelpers.forEach(arrayQueryModifiers, function (modifier) {
            if (value && lxHelpers.isArray(value[modifier])) {
                value[modifier] = lxHelpers.arrayMap(value[modifier], function (item) {
                    if (lxHelpers.isString(item)) {
                        return ObjectID.createFromHexString(item);
                    }

                    if (lxHelpers.isObject(item)) {
                        return item;
                    }
                });

                return false;
            }
        });

    }

    /**
     * Converts all fields in the query to a MongoDB ObjectID if the field is in the idFields.
     *
     * @param {Object=} query The mongoDB query.
     * @return {Object}
     */
    function convertToMongoId (query) {
        query = query || {};

        lxHelpers.forEach(idFields, function (value, key) {
            if (query.hasOwnProperty(key) && lxHelpers.isString(query[key])) {
                query[key] = ObjectID.createFromHexString(query[key]);
            }

            // arrays
            if (query.hasOwnProperty(key) && query[key]) {
                convertMongoIdsInArray(query[key]);
            }

            // $not
            if (query.hasOwnProperty(key) && query[key] && query[key].$not && lxHelpers.isString(query[key].$not)) {
                query[key].$not = ObjectID.createFromHexString(query[key].$not);
            }

            // $ne
            if (query.hasOwnProperty(key) && query[key] && query[key].$ne && lxHelpers.isString(query[key].$ne)) {
                query[key].$ne = ObjectID.createFromHexString(query[key].$ne);
            }
        });

        // $or
        if (query.$or) {
            lxHelpers.forEach(query.$or, function (item) {
                convertToMongoId(item);
            });
        }

        // $and
        if (query.$and) {
            lxHelpers.forEach(query.$and, function (item) {
                convertToMongoId(item);
            });
        }

        // $nor
        if (query.$nor) {
            lxHelpers.forEach(query.$nor, function (item) {
                convertToMongoId(item);
            });
        }

        return query;
    }

    /**
     * Checks if the given options is a mongodb options object.
     *
     * @param {Object} options The query object.
     * @returns {boolean}
     */
    function isMongoDBOptionsObject (options) {
        var isOption = false;
        var mongodbOptions = ['fields', 'sort', 'skip', 'limit', 'w', 'journal', 'wtimeout', 'single', 'fsync'];

        lxHelpers.forEach(options, function (value, key) {
            if (lxHelpers.arrayHasItem(mongodbOptions, key)) {
                isOption = true;
                return false;
            }
        });

        return isOption;
    }

    /**
     * Deletes the key of the document for update operation.
     *
     * @param {object=} document The document.
     */
    function deleteDocumentKey (document) {
        document = document || {};

        if (document.$set) {
            delete document.$set[key];
        } else {
            delete document[key];
        }
    }

    /**
     * Converts a value by format. Is used in lx-valid.
     *
     * @param {String} format The schema format.
     * @param {*} value The value to convert.
     * @returns {*}
     */
    function convert (format, value) {
        if (!lxHelpers.isString(value)) {
            return value;
        }

        if (format === 'mongo-id') {
            return ObjectID.createFromHexString(value);
        }

        if (format === 'date-time' || format === 'date') {
            return new Date(value);
        }

        return value;
    }

    /**
     * Gets the sort option for mongoDB.
     *
     * @param {String|Array|Object} value The sort value.
     * @returns {*}
     */
    function getSort (value) {
        var result = {};

        // return default sort
        if (!value) {
            return sort;
        }

        // sort by the given string ascending, e.g. 'name'
        if (lxHelpers.isString(value)) {
            result[value] = 1;
            return result;
        }

        if (lxHelpers.isArray(value) && value.length > 0) {
            if (lxHelpers.isArray(value[0])) {
                // sort by array, e.g. [['name': 1], ['city': -1]]
                return value;
            }

            lxHelpers.forEach(value, function (item) {
                if (lxHelpers.isString(item)) {
                    result[item] = 1;
                }
            });

            // sort by the strings in the array ascending, e.g. ['name', 'city']
            return result;
        }

        // sort by object, e.g. {name: 1, city: -1}
        if (lxHelpers.isObject(value)) {
            return value;
        }

        return null;
    }

    /**
     * Returns the schema of the collection.
     *
     * @returns {!Object}
     */
    pub.getSchema = function () {
        return lxHelpers.isFunction(schema) ? schema() : schema;
    };

    /**
     * Returns the collection.
     *
     * @returns {!Object}
     */
    pub.getCollection = function () {
        return collection;
    };

    /**
     * Returns the default validation options for lx-valid.
     *
     * @returns {{deleteUnknownProperties: boolean, convert: Function, trim: boolean, strictRequired: boolean}}
     */
    pub.getValidationOptions = function () {
        return {
            // deletes all properties not defined in the json schema
            deleteUnknownProperties: true,
            // function to convert the values with a format to a value that mongoDb can handle (e.g dates, ObjectID)
            convert: convert,
            // trim all values which are in schema and of type 'string'
            trim: true,
            // handle empty string values as invalid when they are required in schema
            strictRequired: true
        };
    };

    /**
     * Creates a new mongo ObjectID
     *
     * @returns {ObjectID}
     */
    pub.createNewId = function () {
        return new ObjectID();
    };

    /**
     * Converts a string in a mongo ObjectID and vice versa.
     *
     * @param {String|ObjectID} id The id to convert.
     * @returns {String|ObjectID}
     */
    pub.convertId = function (id) {
        if (lxHelpers.isString(id)) {
            id = ObjectID.createFromHexString(id);
        } else if (lxHelpers.isObject(id)) {
            id = id.toHexString();
        }

        return id;
    };

    /**
     * Gets the count of the collection.
     *
     * @param {Object|function(err, res)=} query The query/options object or callback.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {!function(err, res)} callback The callback.
     */
    pub.count = function (query, options, callback) {
        if (arguments.length === 1) {
            callback = query;
            options = {};
            query = {};
        }

        if (arguments.length === 2) {
            callback = options;

            if (isMongoDBOptionsObject(query)) {
                options = query;
                query = {};
            } else {
                options = {};
            }
        }

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (!lxHelpers.isObject(query)) {
            return callback(lxHelpers.getTypeError('query', query, {}));
        }

        query = convertToMongoId(query);
        collection.count(query, options, callback);
    };

    /**
     * Inserts a single document or a an array of documents into MongoDB.
     *
     * @param {!Object|!Array.<Object>} docs The document/s.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {function(err, res)=} callback The callback.
     * @deprecated Use insertOne() or insertMany().
     */
    pub.insert = function (docs, options, callback) {
        // deprecated message
        console.log('The method insert() is deprecated. It will be removed in the mongodb node.js driver 3.0. Use insertOne() or insertMany() instead.');

        var error;

        if (arguments.length === 2) {
            if (lxHelpers.isFunction(options)) {
                callback = options;
            }
        }

        options = options || {};

        if (!(lxHelpers.isObject(docs) || lxHelpers.isArray(docs))) {
            error = new TypeError('Param "doc" is of type ' + lxHelpers.getType(docs) + '! Type ' + lxHelpers.getType({}) + ' or ' + lxHelpers.getType([]) + ' expected');

            if (callback && lxHelpers.isFunction(callback)) {
                return callback(error);
            }

            throw error;
        }

        if (arguments.length === 2 && !(lxHelpers.isObject(options) || lxHelpers.isFunction(options))) {
            throw new TypeError('Param "options" is of type ' + lxHelpers.getType(options) + '! Type ' + lxHelpers.getType({}) + ' or ' + lxHelpers.getType(function () {}) + ' expected');
        }

        if (callback && !lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (callback) {
            collection.insert(docs, options, callback);
        } else {
            collection.insert(docs, options);
        }
    };

    /**
     * Inserts an array of documents into MongoDB.
     *
     * @param {!Array.<Object>} docs The documents.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {function(err, res)=} callback The callback.
     * @deprecated Use insertOne() or insertMany().
     */
    pub.insertMany = function (docs, options, callback) {
        if (arguments.length === 2) {
            if (lxHelpers.isFunction(options)) {
                callback = options;
                options = {};
            }
        }

        options = options || {};

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        collection.insertMany(docs, options, callback);
    };

    /**
     * Inserts a single document into MongoDB.
     *
     * @param {!Object} doc The document.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {function(err, res)=} callback The callback.
     * @deprecated Use insertOne() or insertMany().
     */
    pub.insertOne = function (doc, options, callback) {
        if (arguments.length === 2) {
            if (lxHelpers.isFunction(options)) {
                callback = options;
                options = {};
            }
        }

        options = options || {};

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        collection.insertOne(doc, options, callback);
    };

    /**
     * Gets all documents of the collection.
     *
     * @param {Object|function(err, res)=} query The query/options object or the callback.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {Number=} options.skip The skip param for mongoDB.
     * @param {Number=} options.limit The limit param for mongoDB.
     * @param {Array|object=} options.fields The fields which returns from the query.
     * @param {string|Array|object=} options.sort The sort param for mongoDB.
     * @param {!function(err, res)} callback The callback.
     */
    pub.find = function (query, options, callback) {
        if (arguments.length === 1) {
            callback = query;
            query = {};
            options = {};
        }

        if (arguments.length === 2) {
            callback = options;

            if (isMongoDBOptionsObject(query)) {
                options = query;
                query = {};
            } else {
                options = {};
            }
        }

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (!lxHelpers.isObject(query)) {
            return callback(lxHelpers.getTypeError('query', query, {}));
        }

        var mongoOptions = {
            skip: options.skip || 0,
            limit: options.limit || 0,
            fields: options.fields,
            sort: getSort(options.sort)
        };

        query = convertToMongoId(query);
        collection.find(query, mongoOptions).toArray(callback);
    };

    /**
     * Gets one document by query
     *
     * @param {!Object} query The query object.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {Number=} options.skip The skip param for mongoDB.
     * @param {Number=} options.limit The limit param for mongoDB.
     * @param {Array|Object=} options.fields The fields which returns from the query.
     * @param {string|Array|Object=} options.sort The sort param for mongoDB.
     * @param {!function(err, res)} callback The callback.
     */
    pub.findOne = function (query, options, callback) {
        if (arguments.length < 2) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (arguments.length === 2) {
            callback = options;

            if (isMongoDBOptionsObject(query)) {
                options = query;
                query = {};
            } else {
                options = {};
            }
        }

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (!lxHelpers.isObject(query)) {
            return callback(lxHelpers.getTypeError('query', query, {}));
        }

        var mongoOptions = {
            fields: options.fields,
            sort: getSort(options.sort)
        };

        query = convertToMongoId(query);
        collection.findOne(query, mongoOptions, callback);
    };

    /**
     * Gets one document by id
     *
     * @param {ObjectID|String} id The id.
     * @param {Object|function(err, res)=} options The options object or the callback.
     * @param {Array|object} options.fields The fields which returns from the query.
     * @param {function(Error, res)=} callback The callback.
     */
    pub.findOneById = function (id, options, callback) {
        if (arguments.length < 2) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (arguments.length === 2) {
            callback = options;
            options = {};
        }

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (!id || !lxHelpers.isObject(id) && !lxHelpers.isString(id)) {
            return callback(new TypeError('Param "id" is of type ' + lxHelpers.getType(id) + '! Type ' + lxHelpers.getType({}) + ' or ' + lxHelpers.getType('') + ' expected'));
        }

        var mongoOptions = {
            fields: options.fields
        };
        var query = {};
        query[key] = lxHelpers.isString(id) ? pub.convertId(id) : id;

        collection.findOne(query, mongoOptions, callback);
    };

    /**
     * Updates documents.
     *
     * @param {!Object} selector The selector object.
     * @param {!Object} update The new data.
     * @param {Object=} options The options for multi update.
     * @param {!function(err, res)} callback The callback.
     * @deprecated Use updateOne() or updateMany().
     */
    pub.update = function (selector, update, options, callback) {
        // deprecated message
        console.log('The method update() is deprecated. It will be removed in the mongodb node.js driver 3.0. Use updateOne() or updateMany() instead.');

        selector = convertToMongoId(selector);

        // delete key property
        deleteDocumentKey(update);

        options = options || {};
        collection.update(selector, update, options, callback);
    };

    /**
     * Update multiple documents on MongoDB.
     *
     * @param {!Object} filter The filter object.
     * @param {!Object} update The new data.
     * @param {Object=} options The options for multi update.
     * @param {!function(err, res)} callback The callback.
     */
    pub.updateMany = function (filter, update, options, callback) {
        filter = convertToMongoId(filter);

        // delete key property
        deleteDocumentKey(update);

        options = options || {};
        collection.updateMany(filter, update, options, callback);
    };

    /**
     * Updates a single document on MongoDB.
     *
     * @param {!Object} filter The filter object.
     * @param {!Object} update The new data.
     * @param {Object=} options The options for multi update.
     * @param {!function(err, res)} callback The callback.
     */
    pub.updateOne = function (filter, update, options, callback) {
        filter = convertToMongoId(filter);

        // delete key property
        deleteDocumentKey(update);

        options = options || {};
        collection.update(filter, update, options, callback);
    };

    /**
     * Deletes the documents of the query.
     *
     * @param {Object=} query The query object.
     * @param {Object=} options The options object.
     * @param {function(object, object)=} callback The callback.
     * @deprecated Use deleteOne() or deleteMany() instead.
     */
    pub.remove = function (query, options, callback) {
        // deprecated message
        console.log('The method remove() is deprecated. It will be removed in the mongodb node.js driver 3.0. Use deleteOne() or deleteMany() instead.');

        var error;

        if (arguments.length === 1) {
            if (lxHelpers.isFunction(query)) {
                callback = query;
                query = {};
                options = {};
            }

            if (isMongoDBOptionsObject(query)) {
                options = query;
                query = {};
            }
        }

        if (arguments.length === 2) {
            if (lxHelpers.isFunction(options)) {
                callback = options;
                options = {};
            }

            if (isMongoDBOptionsObject(query)) {
                options = query;
                query = {};
            }
        }

        options = options || {};

        if (lxHelpers.getType(query) !== 'undefined' && !lxHelpers.isObject(query)) {
            error = lxHelpers.getTypeError('query', query, {});

            if (callback && lxHelpers.isFunction(callback)) {
                return callback(error);
            }

            throw error;
        }

        if (options && !lxHelpers.isObject(options)) {
            error = lxHelpers.getTypeError('options', options, {});

            if (callback && lxHelpers.isFunction(callback)) {
                return callback(error);
            }

            throw error;
        }

        query = convertToMongoId(query);

        if (callback && !lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (callback) {
            collection.remove(query, options, callback);
        } else {
            collection.remove(query, options);
        }
    };

    /**
     * Delete multiple documents on MongoDB.
     *
     * @param {Object=} filter The filter object.
     * @param {Object=} options The options object.
     * @param {function(object, object)=} callback The callback.
     * @deprecated Use deleteOne() or deleteMany() instead.
     */
    pub.deleteMany = function (filter, options, callback) {
        // only callback given, delete all documents
        if (arguments.length === 1 && lxHelpers.isFunction(filter)) {
            callback = filter;
            filter = null;
            options = null;
        }

        if (arguments.length === 2 && lxHelpers.isFunction(options)) {
            callback = options;
            options = null;
        }

        filter = convertToMongoId(filter);

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        collection.deleteMany(filter, options, callback);
    };

    /**
     * Delete a document on MongoDB.
     *
     * @param {Object=} filter The filter object.
     * @param {Object=} options The options object.
     * @param {function(object, object)=} callback The callback.
     * @deprecated Use deleteOne() or deleteMany() instead.
     */
    pub.deleteOne = function (filter, options, callback) {
        // only callback given, delete all documents
        if (arguments.length === 1 && lxHelpers.isFunction(filter)) {
            callback = filter;
            filter = null;
            options = null;
        }

        if (arguments.length === 2 && lxHelpers.isFunction(options)) {
            callback = options;
            options = null;
        }

        filter = convertToMongoId(filter);

        if (!lxHelpers.isFunction(callback)) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        collection.deleteOne(filter, options, callback);
    };

    /**
     * Execute an aggregation framework pipeline against the collection.
     *
     * @param {Array} pipeline The aggregation framework pipeline.
     * @param {Object=} options The additional options.
     * @param {function(err, res)} callback The callback.
     */
    pub.aggregate = function (pipeline, options, callback) {
        if (arguments.length < 2) {
            throw lxHelpers.getTypeError('callback', callback, Function);
        }

        if (arguments.length === 2) {
            callback = options;
            options = {};
        }

        if (!lxHelpers.isArray(pipeline)) {
            return callback(lxHelpers.getTypeError('pipeline', pipeline, []));
        }

        options = options || {};

        collection.aggregate(pipeline, options, callback);
    };

    var tmpSchema = pub.getSchema();

    if (tmpSchema.hasOwnProperty('properties')) {
        analyseSchema(tmpSchema.properties);
    } else {
        analyseSchema(tmpSchema);
    }

    // set default sorting
    if (Object.keys(sort).length === 0) {
        sort[key] = 1;
    }

    return pub;
};
