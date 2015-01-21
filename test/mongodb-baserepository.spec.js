'use strict';

var path = require('path');
var lxHelpers = require('lx-helpers');
var sut = require(path.join(process.cwd(), 'lib', 'mongodb-baserepository'));
var userRepo = require(path.join(process.cwd(), 'test', 'fixtures', 'usersRepository.js'));
var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
var MongoError = require(path.join(process.cwd(), 'node_modules', 'mongodb', 'node_modules', 'mongodb-core')).MongoError;
var db, user;
var pipeline = [
    {
        $project: {
            age: 1
        }
    },
    {
        $group: {
            _id: {age: '$age'},
            count: {$sum: 1}
        }
    },
    {
        $project: {
            _id: '$_id',
            age: '$age',
            count: '$count'
        }
    }
];

MongoClient.connect('mongodb://localhost:27017/mongodb-baserepository-test?w=1&journal=True&fsync=True', function (err, res) {
    if (res) {
        db = res;
    }
});

describe('Connect to db', function () {
    beforeEach(function (done) {
        setTimeout(function () {
            // wait for db to be connected
            if (db) {
                done();
            }

        }, 1000);
    });

    it('.start()', function () {
        expect(true).toBeTruthy();
    });
});

describe('MongoDB BaseRepository', function () {

    beforeEach(function (done) {
        user = {
            firstName: 'Chuck',
            lastName: 'Norris',
            userName: 'chuck',
            email: 'chuck@norris.com',
            birthdate: new Date(2000, 10, 10),
            age: 20
        };

        db.collection('users').deleteMany({}, function () {
            done();
        });
    });

    it('should throw an exception when parameter "collection" is empty', function () {
        function func1 () {
            return sut(null);
        }

        function func2 () {
            return sut(undefined);
        }

        function func3 () {
            return sut(false);
        }

        function func4 () {
            return sut();
        }

        expect(func1).toThrow();
        expect(func2).toThrow();
        expect(func3).toThrow();
        expect(func4).toThrow();
    });

    it('should analyse the schema and set the indexes', function (done) {
        var repo = userRepo(db.collection('users'));

        setTimeout(function () {
            // Fetch basic indexInformation for collection
            repo.getCollection().indexInformation({full: true}, function (err, indexInformation) {
                expect(err).toBeNull();
                expect(indexInformation.length).toBe(6);

                var i;
                var length = indexInformation.length;

                for (i = 0; i < length; i++) {
                    var idx = indexInformation[i];

                    if (idx.name === '_id_') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {_id: 1},
                            ns: 'mongodb-baserepository-test.users',
                            name: '_id_'
                        });
                    }

                    if (idx.name === 'indexProp_1') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {indexProp: 1},
                            ns: 'mongodb-baserepository-test.users',
                            name: 'indexProp_1'
                        });
                    }

                    if (idx.name === 'uniqueProp_1') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {uniqueProp: 1},
                            ns: 'mongodb-baserepository-test.users',
                            name: 'uniqueProp_1',
                            unique: true
                        });
                    }

                    if (idx.name === 'userName_1') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {'a.aa.name': 1},
                            ns: 'mongodb-baserepository-test.users',
                            name: 'userName_1',
                            unique: true
                        });
                    }

                    if (idx.name === 'a.aa.aaa.aaaa.name_-1') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {'a.aa.aaa.aaaa.name': -1},
                            ns: 'mongodb-baserepository-test.users',
                            name: 'a.aa.aaa.aaaa.name_-1'
                        });
                    }

                    if (idx.name === 'i.ii.iii.iiii.name_1') {
                        expect(idx).toEqual({
                            v: 1,
                            key: {'i.ii.iii.iiii.name': 1},
                            ns: 'mongodb-baserepository-test.users',
                            name: 'i.ii.iii.iiii.name_1'
                        });
                    }
                }

                // drop collection to remove indexes
                db.collection('users').drop(function () {
                    done();
                });
            });
        }, 1000);
    });

    it('.getCollection() should return the collection', function () {
        var repo = sut(db.collection('users'));

        expect(typeof repo.getCollection()).toBe('object');
    });

    it('.createNewId() should return a new MongoDB ObjectID', function () {
        var repo = sut(db.collection('users'));

        expect(typeof repo.createNewId()).toBe('object');
        expect(repo.createNewId() instanceof ObjectID).toBeTruthy();
    });

    it('.convertId() should convert a MongoDB ObjectID to a string and vice versa', function () {
        var repo = sut(db.collection('users'));
        var id = '5108e9333cb086801f000035';
        var _id = new ObjectID(id);

        expect(typeof repo.convertId(id)).toBe('object');
        expect(repo.convertId(id) instanceof ObjectID).toBeTruthy();

        expect(typeof repo.convertId(_id)).toBe('string');
        expect(repo.convertId(_id)).toBe(id);
    });

    it('.getSchema() should return the schema', function () {
        var repo = sut(db.collection('users'));
        var schema = repo.getSchema();
        var userSchema = userRepo(db.collection('users')).getSchema();

        expect(typeof schema).toBe('object');
        expect(typeof userSchema).toBe('object');

        userSchema.properties.userName.required = false;

        var userSchema2 = userRepo(db.collection('users')).getSchema();

        expect(typeof userSchema2).toBe('object');
        expect(userSchema2.properties.userName.required).toBeTruthy();
    });

    it('.getValidationOptions() should return the options for validating', function () {
        var repo = sut(db.collection('users'));
        var res = repo.getValidationOptions();

        expect(res.unknownProperties).toBeTruthy();
        expect(res.trim).toBeTruthy();
        expect(res.strictRequired).toBeTruthy();
        expect(typeof res.convert).toBe('function');
    });

    it('.convert() should convert values', function () {
        var convert = sut(db.collection('users')).getValidationOptions().convert,
            res = convert('mongo-id', '507f191e810c19729de860ea'),
            res2 = convert('date-time', '1973-06-01T15:49:00.000Z'),
            res3 = convert('date', '1973-06-01'),
            res4 = convert(null, '132'),
            res5 = convert(null, 111);

        expect(lxHelpers.isObject(res)).toBeTruthy();
        expect(res.toHexString()).toBe('507f191e810c19729de860ea');
        expect(lxHelpers.isDate(res2)).toBeTruthy();
        expect(res2).toEqual(new Date('1973-06-01T15:49:00.000Z'));
        expect(lxHelpers.isDate(res3)).toBeTruthy();
        expect(res3).toEqual(new Date('1973-06-01'));
        expect(res4).toBe('132');
        expect(res5).toBe(111);
    });

    it('should convert all mongo-ids', function (done) {
        var schema = {
            properties: {
                _id: {
                    type: 'string',
                    format: 'mongo-id'
                },
                _id1: {
                    type: 'string',
                    format: 'mongo-id'
                },
                _id2: {
                    type: 'string',
                    format: 'mongo-id'
                },
                a: {
                    type: 'object',
                    properties: {
                        aa: {
                            type: 'string',
                            format: 'mongo-id'
                        }
                    }
                },
                in: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                all: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                nin: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                elem: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            cc: {
                                type: 'string',
                                format: 'mongo-id'
                            }
                        }
                    }
                },
                d: {
                    type: 'object',
                    properties: {
                        dd: {
                            type: 'object',
                            properties: {
                                ddd: {
                                    type: 'object',
                                    properties: {
                                        dddd: {
                                            type: 'string',
                                            format: 'mongo-id'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        var collectionMock = db.collection('users');
        collectionMock.count = function (query, options, cb) {
            expect(query._id instanceof ObjectID).toBeTruthy();
            expect(query['a.aa'] instanceof ObjectID).toBeTruthy();
            expect(query['d.dd.ddd.dddd'] instanceof ObjectID).toBeTruthy();
            expect(query.in.$in[0] instanceof ObjectID).toBeTruthy();
            expect(query.all.$all[0] instanceof ObjectID).toBeTruthy();
            expect(query.nin.$nin[0] instanceof ObjectID).toBeTruthy();
            expect(query.nin.$nin[1] instanceof ObjectID).toBeTruthy();
            expect(query.$or[0]._id instanceof ObjectID).toBeTruthy();
            expect(query.$or[1]['a.aa'] instanceof ObjectID).toBeTruthy();
            expect(query.$and[0]._id instanceof ObjectID).toBeTruthy();
            expect(query.$and[0]._id instanceof ObjectID).toBeTruthy();
            expect(query.$nor[0]._id instanceof ObjectID).toBeTruthy();
            expect(query.$nor[1]['a.aa'] instanceof ObjectID).toBeTruthy();
            expect(query['elem.cc'] instanceof ObjectID).toBeTruthy();
            expect(query._id1.$not instanceof ObjectID).toBeTruthy();
            expect(query._id2.$ne instanceof ObjectID).toBeTruthy();

            cb();
        };

        var repo = sut(collectionMock, schema);
        var query = {
            _id: '507f191e810c19729de860ea',
            'a.aa': '507f191e810c19729de860ea',
            'd.dd.ddd.dddd': '507f191e810c19729de860ea',
            in: {
                $in: ['507f191e810c19729de860ea']
            },
            all: {
                $all: ['507f191e810c19729de860ea']
            },
            nin: {
                $nin: ['507f191e810c19729de860ea', '507f191e810c19729de860ea']
            },
            $or: [{_id: '507f191e810c19729de860ea'}, {'a.aa': '507f191e810c19729de860ea'}],
            'elem.cc': '507f191e810c19729de860ea',
            $and: [{_id: '507f191e810c19729de860ea'}, {'a.aa': '507f191e810c19729de860ea'}],
            $nor: [{_id: '507f191e810c19729de860ea'}, {'a.aa': '507f191e810c19729de860ea'}],
            _id1: {
                $not: '507f191e810c19729de860ea'
            },
            _id2: {
                $ne: '507f191e810c19729de860ea'
            }
        };

        repo.count(query, function () {
            done();
        });
    });

    it('should convert all mongo-ids in a complex $or condition', function (done) {
        var schema = {
            properties: {
                _id: {
                    type: 'string',
                    format: 'mongo-id'
                },
                _id1: {
                    type: 'string',
                    format: 'mongo-id'
                },
                _id2: {
                    type: 'string',
                    format: 'mongo-id'
                },
                a: {
                    type: 'object',
                    properties: {
                        aa: {
                            type: 'string',
                            format: 'mongo-id'
                        }
                    }
                },
                in: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                all: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                nin: {
                    type: 'array',
                    items: {
                        type: 'string',
                        format: 'mongo-id'
                    }
                },
                elem: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            cc: {
                                type: 'string',
                                format: 'mongo-id'
                            }
                        }
                    }
                },
                d: {
                    type: 'object',
                    properties: {
                        dd: {
                            type: 'object',
                            properties: {
                                ddd: {
                                    type: 'object',
                                    properties: {
                                        dddd: {
                                            type: 'string',
                                            format: 'mongo-id'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        var collectionMock = db.collection('users');
        collectionMock.count = function (query, options, cb) {
            expect(query.$or[0]._id.$not instanceof ObjectID).toBeTruthy();
            expect(query.$or[1]['d.dd.ddd.dddd'] instanceof ObjectID).toBeTruthy();

            cb();
        };

        var repo = sut(collectionMock, schema);
        var query = {
            $or: [
                {
                    _id: {
                        $not: '507f191e810c19729de860ea'
                    }
                },
                {
                    'd.dd.ddd.dddd': '507f191e810c19729de860ea'
                }
            ]
        };

        repo.count(query, function () {
            done();
        });
    });

    describe('.count()', function () {
        it('should throw an exception if the params are wrong', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.count(null);
            }).toThrow();

            expect(function () {
                return repo.count({}, {}, null);
            }).toThrow();

            expect(function () {
                return repo.count({}, null);
            }).toThrow();
        });

        it('should return the number of documents of the collection in the BaseRepo', function (done) {
            var repo = sut(db.collection('users'));

            repo.count(function (error, result) {
                expect(result).toBe(0);

                repo.insert(user, function (err) {
                    expect(err).toBeNull();

                    repo.insert({userName: 'wayne'}, function (err) {
                        expect(err).toBeNull();

                        repo.count(function (error, result) {
                            expect(result).toBe(2);

                            repo.count({userName: 'wayne'}, function (error, result) {
                                expect(result).toBe(1);

                                repo.count({limit: 1}, function (error, result) {
                                    expect(result).toBe(1);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should return an error callback when the param "query" is not of type object', function (done) {
            var repo = sut(db.collection('users'));

            repo.count(123, function (error, result) {
                expect(result).toBeUndefined();
                expect(error).toBeDefined();
                expect(error instanceof TypeError).toBeTruthy();
                expect(error.message).toBe('Param "query" is of type number! Type object expected');

                done();
            });
        });
    });

    describe('.insert()', function () {
        it('should insert a new document in the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function (error, result) {
                expect(result).toBeDefined();
                expect(Array.isArray(result.ops)).toBeTruthy();
                expect(result.ops[0].firstName).toBe('Chuck');
                expect(result.ops[0].lastName).toBe('Norris');
                expect(result.ops[0].userName).toBe('chuck');
                expect(result.ops[0].email).toBe('chuck@norris.com');
                expect(result.ops[0].birthdate instanceof Date).toBeTruthy();
                expect(result.result.n).toBe(1);
                expect(result.result.ok).toBe(1);

                done();
            });
        });

        it('should insert a new document in the collection without callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user);

            setTimeout(function () {
                repo.find({userName: 'chuck'}, function (err, result) {
                    expect(err).toBeNull();
                    expect(result).toBeDefined();
                    expect(Array.isArray(result)).toBeTruthy();
                    expect(result[0].firstName).toBe('Chuck');
                    expect(result[0].lastName).toBe('Norris');
                    expect(result[0].userName).toBe('chuck');
                    expect(result[0].email).toBe('chuck@norris.com');
                    expect(result[0].birthdate instanceof Date).toBeTruthy();

                    done();
                });
            }, 300);
        });

        it('should insert a new document in the collection with options and without callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, {w: 0});

            setTimeout(function () {
                repo.find({userName: 'chuck'}, function (err, result) {
                    expect(result).toBeDefined();
                    expect(Array.isArray(result)).toBeTruthy();
                    expect(result[0].firstName).toBe('Chuck');
                    expect(result[0].lastName).toBe('Norris');
                    expect(result[0].userName).toBe('chuck');
                    expect(result[0].email).toBe('chuck@norris.com');
                    expect(result[0].birthdate instanceof Date).toBeTruthy();
                    done();
                });
            }, 100);
        });

        it('should insert an array with new documents in the collection', function (done) {
            var repo = sut(db.collection('users'));
            var doc = [user, {userName: 'test'}, {lastName: 'wayne'}];

            repo.insert(doc, function (error, result) {
                expect(error).toBeNull();
                expect(result).toBeDefined();
                expect(Array.isArray(result.ops)).toBeTruthy();
                expect(result.ops.length).toBe(3);
                expect(result.ops[0].firstName).toBe('Chuck');
                expect(result.ops[0].lastName).toBe('Norris');
                expect(result.ops[0].userName).toBe('chuck');
                expect(result.ops[0].email).toBe('chuck@norris.com');
                expect(result.ops[0].birthdate instanceof Date).toBeTruthy();
                expect(result.result.n).toBe(3);
                expect(result.result.ok).toBe(1);

                done();
            });
        });

        it('should return an error callback when the param "doc" is not of type object or array', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(123, function (error, result) {
                expect(result).toBeUndefined();
                expect(error).toBeDefined();
                expect(error instanceof TypeError).toBeTruthy();
                expect(error.message).toBe('Param "doc" is of type number! Type object or array expected');

                done();
            });
        });

        it('should throw an exception if the params are wrong', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.insert(null);
            }).toThrow();

            expect(function () {
                return repo.insert({}, {}, 2);
            }).toThrow();

            expect(function () {
                return repo.insert({}, 1);
            }).toThrow();
        });
    });

    describe('.insertOne()', function () {
        it('should insert a new document in the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insertOne(user, function (error, result) {
                expect(result).toBeDefined();
                expect(Array.isArray(result.ops)).toBeTruthy();
                expect(result.ops[0].firstName).toBe('Chuck');
                expect(result.ops[0].lastName).toBe('Norris');
                expect(result.ops[0].userName).toBe('chuck');
                expect(result.ops[0].email).toBe('chuck@norris.com');
                expect(result.ops[0].birthdate instanceof Date).toBeTruthy();
                expect(result.result.n).toBe(1);
                expect(result.result.ok).toBe(1);

                done();
            });
        });

        it('should return an error callback when the param "doc" is an array', function (done) {
            var repo = sut(db.collection('users'));

            repo.insertOne([user], function (error, result) {
                expect(result).toBeUndefined();
                expect(error instanceof MongoError).toBeTruthy();
                expect(error.message).toBe('doc parameter must be an object');

                done();
            });
        });

        it('should throw an exception if the params are wrong', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.insertOne(null);
            }).toThrow();

            expect(function () {
                return repo.insertOne({}, {}, 2);
            }).toThrow();

            expect(function () {
                return repo.insertOne({}, 1);
            }).toThrow();
        });
    });

    describe('.insertMany()', function () {
        it('should return an error callback when the param "docs" is not an array', function (done) {
            var repo = sut(db.collection('users'));

            repo.insertMany(user, function (error, result) {
                expect(result).toBeUndefined();
                expect(error instanceof MongoError).toBeTruthy();
                expect(error.message).toBe('docs parameter must be an array of documents');

                done();
            });
        });

        it('should insert new documents in the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insertMany([user, {userName: 'test'}, {lastName: 'wayne'}], function (error, result) {
                expect(error).toBeNull();
                expect(lxHelpers.isObject(result)).toBeTruthy();
                expect(Array.isArray(result.ops)).toBeTruthy();
                expect(result.ops[0].firstName).toBe('Chuck');
                expect(result.ops[0].lastName).toBe('Norris');
                expect(result.ops[0].userName).toBe('chuck');
                expect(result.ops[0].email).toBe('chuck@norris.com');
                expect(result.ops[0].birthdate instanceof Date).toBeTruthy();
                expect(result.result.n).toBe(3);
                expect(result.result.ok).toBe(1);

                done();
            });
        });

        it('should throw an exception if the params are wrong', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.insertMany(null);
            }).toThrow();

            expect(function () {
                return repo.insertMany({}, {}, 2);
            }).toThrow();

            expect(function () {
                return repo.insertMany({}, 1);
            }).toThrow();
        });
    });

    describe('has a function find() which', function () {
        it('should throw an exception if the params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.find();
            }).toThrow();

            expect(function () {
                return repo.find(1, 2);
            }).toThrow();

            expect(function () {
                return repo.find(1, 2, 3);
            }).toThrow();
        });

        it('should get all documents of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find(function (err, res) {
                        expect(err).toBeNull();
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);
                        expect(res[0].userName).toBe('chuck');
                        expect(res[1].userName).toBe('wayne');

                        done();
                    });
                });
            });
        });

        it('should callback with an error if the param "query" is not an object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find(123, function (err, res) {
                        expect(err).toBeDefined();
                        expect(err instanceof TypeError).toBeTruthy();
                        expect(res).toBeUndefined();

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection and check the query', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({limit: 1}, function (err, res) {
                        expect(err).toBeNull();
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection and convert $in in query with ids', function (done) {
            var ids = [];
            var repo = userRepo(db.collection('users'));

            repo.insert(user, function (err, res) {
                ids.push(res.ops[0]._id.toString());
                repo.insert({userName: 'wayne'}, function (err, res) {
                    ids.push(res.ops[0]._id.toString());
                    repo.insert({userName: 'who'}, function () {
                        repo.find({_id: {$in: ids}}, function (err, res) {
                            expect(err).toBeNull();
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(2);
                            expect(res[0].userName).toBe('chuck');
                            expect(res[1].userName).toBe('wayne');

                            done();
                        });
                    });
                });
            });
        });

        it('should get all documents of the collection and not convert $in in query with ids when ids are already mongo-ids', function (done) {
            var ids = [];
            var repo = userRepo(db.collection('users'));

            repo.insert(user, function (err, res) {
                ids.push(res.ops[0]._id);
                repo.insert({userName: 'wayne'}, function (err, res) {
                    ids.push(res.ops[0]._id);
                    repo.insert({userName: 'who'}, function () {
                        repo.find({_id: {$in: ids}}, function (err, res) {
                            expect(err).toBeNull();
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(2);
                            expect(res[0].userName).toBe('chuck');
                            expect(res[1].userName).toBe('wayne');

                            done();
                        });
                    });
                });
            });
        });

        it('should get all documents of the collection with filter', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({userName: 'wayne'}, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with limit', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, {limit: 1}, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with skip', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, {skip: 1}, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(1);
                        expect(res[0].userName).toBe('wayne');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with the specified fields Array', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                fields: ['userName', 'lastName']
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, options, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('chuck');
                        expect(res[0].lastName).toBe('Norris');
                        expect(res[0]._id).toBeDefined();
                        expect(res[0].email).toBeUndefined();
                        expect(res[0].birthdate).toBeUndefined();
                        expect(Object.keys(res[0]).length).toBe(3);

                        expect(res[1].userName).toBe('wayne');
                        expect(res[1].lastName).toBeUndefined();
                        expect(res[1].lastName).toBeUndefined();
                        expect(Object.keys(res[1]).length).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with the specified fields Object', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                fields: {
                    'userName': 1,
                    'lastName': 1
                }
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, options, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('chuck');
                        expect(res[0].lastName).toBe('Norris');
                        expect(res[0]._id).toBeDefined();
                        expect(res[0].email).toBeUndefined();
                        expect(res[0].birthdate).toBeUndefined();
                        expect(Object.keys(res[0]).length).toBe(3);

                        expect(res[1].userName).toBe('wayne');
                        expect(res[1].lastName).toBeUndefined();
                        expect(res[1].lastName).toBeUndefined();
                        expect(Object.keys(res[1]).length).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with the specified sorting ascending', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: {'userName': 1}
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, options, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('chuck');
                        expect(res[1].userName).toBe('wayne');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with the specified sorting descending', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: {
                    userName: -1
                }
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, options, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('wayne');
                        expect(res[1].userName).toBe('chuck');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection and sort by string', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: 'userName'
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.find({}, options, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('chuck');
                        expect(res[1].userName).toBe('wayne');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection and sort by array of strings', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: ['userName', 'age']
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 15}, function () {
                    repo.insert({userName: 'wayne2', age: 10}, function () {
                        repo.find({}, options, function (err, res) {
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(3);

                            expect(res[0].userName).toBe('chuck');
                            expect(res[1].userName).toBe('wayne');
                            expect(res[1].age).toBe(15);
                            expect(res[2].userName).toBe('wayne2');
                            expect(res[2].age).toBe(10);

                            done();
                        });
                    });
                });
            });
        });

        it('should get all documents of the collection and sort by array', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: [
                    ['userName', 1],
                    ['age', -1]
                ]
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 10}, function () {
                    repo.insert({userName: 'wayne2', age: 15}, function () {
                        repo.find({}, options, function (err, res) {
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(3);

                            expect(res[0].userName).toBe('chuck');
                            expect(res[1].userName).toBe('wayne');
                            expect(res[1].age).toBe(10);
                            expect(res[2].userName).toBe('wayne2');
                            expect(res[2].age).toBe(15);

                            done();
                        });
                    });
                });
            });
        });

        it('should get all documents of the collection and sort by object', function (done) {
            var repo = sut(db.collection('users'));
            var options = {
                sort: {
                    userName: 1,
                    age: -1
                }
            };

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 10}, function () {
                    repo.insert({userName: 'wayne2', age: 15}, function () {
                        repo.find({}, options, function (err, res) {
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(3);

                            expect(res[0].userName).toBe('chuck');
                            expect(res[1].userName).toBe('wayne');
                            expect(res[1].age).toBe(10);
                            expect(res[2].userName).toBe('wayne2');
                            expect(res[2].age).toBe(15);

                            done();
                        });
                    });
                });
            });
        });

        it('should get all documents of the collection with the default sorting', function (done) {
            var repo = userRepo(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'aaa'}, function () {
                    repo.find(function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('aaa');
                        expect(res[1].userName).toBe('chuck');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection with the default sorting when param sort is empty', function (done) {
            var repo = userRepo(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'aaa'}, function () {
                    repo.find({}, {sort: null}, function (err, res) {
                        expect(Array.isArray(res)).toBeTruthy();
                        expect(res.length).toBe(2);

                        expect(res[0].userName).toBe('aaa');
                        expect(res[1].userName).toBe('chuck');

                        done();
                    });
                });
            });
        });

        it('should get all documents of the collection and convert the id in the query to mongo id', function (done) {
            var repo = userRepo(db.collection('users'));

            repo.insert(user, function (err, res) {
                var id = res.ops[0]._id;

                repo.insert({userName: 'aaa', chief_id: id, i: {ii: {manager_id: id}}}, function (err, res) {
                    expect(err).toBeNull();
                    expect(res).toBeDefined();

                    repo.find({chief_id: id.toHexString()}, function (err, res1) {
                        expect(Array.isArray(res1)).toBeTruthy();
                        expect(res1.length).toBe(1);
                        expect(res1[0].userName).toBe('aaa');
                        expect(res1[0].chief_id.toHexString()).toBe(id.toHexString());

                        repo.find({'i.ii.manager_id': id.toHexString()}, function (err, res1) {
                            expect(Array.isArray(res1)).toBeTruthy();
                            expect(res1.length).toBe(1);
                            expect(res1[0].userName).toBe('aaa');
                            expect(res1[0].i.ii.manager_id.toHexString()).toBe(id.toHexString());

                            done();
                        });
                    });
                });
            });
        });
    });

    describe('has a function findOne() which', function () {
        it('should throw an exception if the params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.findOne();
            }).toThrow();

            expect(function () {
                return repo.findOne({});
            }).toThrow();

            expect(function () {
                return repo.findOne(1, 2);
            }).toThrow();

            expect(function () {
                return repo.findOne(1, 2, 3);
            }).toThrow();
        });

        it('should return one document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne({userName: 'chuck'}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.userName).toBe('chuck');
                        expect(res.lastName).toBe('Norris');

                        done();
                    });
                });
            });
        });

        it('should callback with an error if the param "query" is not an object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne(123, function (err, res) {
                        expect(err).toBeDefined();
                        expect(err instanceof TypeError).toBeTruthy();
                        expect(res).toBeUndefined();

                        done();
                    });
                });
            });
        });

        it('should return one document of the collection and check the query', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne({fields: ['_id']}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res._id).toBeDefined();

                        done();
                    });
                });
            });
        });

        it('should return no document when the collection is empty', function (done) {
            var repo = sut(db.collection('users'));

            repo.findOne({}, function (err, res) {
                expect(res).toBeDefined();
                expect(res).toBeNull();

                done();
            });
        });

        it('should return no document when the query not matches any document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne({userName: 'who?'}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res).toBeNull();

                        done();
                    });
                });
            });
        });
    });

    describe('has a function findOneById() which', function () {
        it('should return one document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne({userName: 'chuck'}, function (err, res) {
                        expect(res).toBeDefined();

                        repo.findOneById(res._id, function (err, res1) {
                            expect(res1).toBeDefined();
                            expect(res1._id.toString()).toBe(res._id.toString());
                            expect(res1.userName).toBe('chuck');
                            done();
                        });
                    });
                });
            });
        });

        it('should return one document of the collection and convert the id to a mongo id', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.findOne({userName: 'chuck'}, function (err, res) {
                        expect(res).toBeDefined();

                        repo.findOneById(res._id.toHexString(), function (err, res1) {
                            expect(err).toBeNull();
                            expect(res1).not.toBeNull();
                            expect(res1._id.toString()).toBe(res._id.toString());
                            expect(res1.userName).toBe('chuck');
                            done();
                        });
                    });
                });
            });
        });

        it('should return no document when the collection is empty', function (done) {
            var repo = sut(db.collection('users'));

            repo.findOneById('5108e9333cb086801f000035', function (err, res) {
                expect(res).toBeDefined();
                expect(res).toBeNull();

                done();
            });
        });

        it('should return no document when the id not matches any document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.findOneById('5108e9333cb086801f000035', function (err, res) {
                expect(res).toBeDefined();
                expect(res).toBe(null);

                done();
            });
        });

        it('should return no document when the id is of wrong type', function (done) {
            var repo = sut(db.collection('users'));

            repo.findOneById(123, function (err, res) {
                expect(err).toBeDefined();
                expect(err instanceof TypeError).toBeTruthy();
                expect(err.message).toBe('Param "id" is of type number! Type object or string expected');
                expect(res).toBeUndefined();

                done();
            });
        });

        it('should return no document when the id undefined or null', function (done) {
            var repo = sut(db.collection('users'));

            repo.findOneById(null, function (err, res) {
                expect(err).toBeDefined();
                expect(err instanceof TypeError).toBeTruthy();
                expect(err.message).toBe('Param "id" is of type null! Type object or string expected');
                expect(res).toBeUndefined();

                repo.findOneById(undefined, function (err, res) {
                    expect(err).toBeDefined();
                    expect(err instanceof TypeError).toBeTruthy();
                    expect(err.message).toBe('Param "id" is of type undefined! Type object or string expected');
                    expect(res).toBeUndefined();

                    done();
                });
            });
        });

        it('should throw an exception when the params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.findOneById(1, 2, 3);
            }).toThrow();

            expect(function () {
                return repo.findOneById(null, undefined, 'test');
            }).toThrow();

            expect(function () {
                return repo.findOneById();
            }).toThrow();
        });
    });

    describe('has a function update() which', function () {
        it('should update the document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.update({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        repo.findOne({userName: 'bob'}, function (err, res1) {
                            expect(res1).toBeDefined();
                            expect(res1.userName).toBe('bob');
                            expect(res1.lastName).toBe('Norris');
                            expect(res1.email).toBe('chuck@norris.com');

                            done();
                        });
                    });
                });
            });
        });

        it('should update no document when the query matches no document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.update({userName: 'chuck1'}, {userName: 'bob'}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(0);

                        done();
                    });
                });
            });
        });

        it('should throw an exception when the number of params is less than 3', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.update(1, 2);
            }).toThrow();

            expect(function () {
                return repo.update(1);
            }).toThrow();

            expect(function () {
                return repo.update();
            }).toThrow();
        });

        it('should update the document of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.update({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update the document of the collection and has no options as parameter', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.update({userName: 'chuck'}, {'$set': {userName: 'bob'}}, null, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update all documents of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck'
            };

            var user2 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck2'
            };

            repo.insert(user1, function () {
                repo.insert(user2, function () {
                    repo.count({userName: 'chuck'}, function (err, res) {
                        expect(res).toBe(1);

                        repo.update({firstName: 'Chuck'}, {'$set': {lastName: 'bob'}}, {multi: true}, function (err, res) {
                            expect(res).toBeDefined();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(2);

                            repo.count({lastName: 'Norris'}, function (err, res) {
                                expect(res).toBe(0);
                                repo.count({lastName: 'bob'}, function (err, res) {
                                    expect(res).toBe(2);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should delete the key of the document on update', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function (err, res) {
                res.ops[0].userName = 'bob';

                repo.update({userName: 'chuck'}, {'$set': res.ops[0]}, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        res1.lastName = 'Wayne';

                        repo.update({userName: 'bob'}, res1, function (err, res2) {
                            expect(res2).toBeDefined();
                            expect(res2.result.ok).toBe(1);
                            expect(res2.result.n).toBe(1);

                            repo.findOne({lastName: 'Wayne'}, function (err, res3) {
                                expect(res3).toBeDefined();
                                expect(res3.userName).toBe('bob');
                                expect(res3.lastName).toBe('Wayne');
                                expect(res3.email).toBe('chuck@norris.com');

                                done();
                            });
                        });
                    });
                });

            });
        });
    });

    describe('has a function updateMany() which', function () {
        it('should update the document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.updateMany({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        repo.findOne({userName: 'bob'}, function (err, res1) {
                            expect(res1).toBeDefined();
                            expect(res1.userName).toBe('bob');
                            expect(res1.lastName).toBe('Norris');
                            expect(res1.email).toBe('chuck@norris.com');

                            done();
                        });
                    });
                });
            });
        });

        it('should update no document when the query matches no document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.updateMany({userName: 'chuck1'}, {userName: 'bob'}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(0);

                        done();
                    });
                });
            });
        });

        it('should throw an exception when the number of params is less than 3', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.updateMany(1, 2);
            }).toThrow();

            expect(function () {
                return repo.updateMany(1);
            }).toThrow();

            expect(function () {
                return repo.updateMany();
            }).toThrow();
        });

        it('should update the document of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.updateMany({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update the document of the collection and has no options as parameter', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.updateMany({userName: 'chuck'}, {'$set': {userName: 'bob'}}, null, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update all documents of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck'
            };

            var user2 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck2'
            };

            repo.insert(user1, function () {
                repo.insert(user2, function () {
                    repo.count({userName: 'chuck'}, function (err, res) {
                        expect(res).toBe(1);

                        repo.updateMany({firstName: 'Chuck'}, {'$set': {lastName: 'bob'}}, {multi: true}, function (err, res) {
                            expect(res).toBeDefined();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(2);

                            repo.count({lastName: 'Norris'}, function (err, res) {
                                expect(res).toBe(0);
                                repo.count({lastName: 'bob'}, function (err, res) {
                                    expect(res).toBe(2);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should return an error callback when the update does not contain a $ operator', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck'
            };

            repo.insert(user1, function () {
                repo.updateMany({userName: 'chuck'}, {lastName: '133'}, function (err, res) {
                    expect(res).toBeUndefined();
                    expect(err.name).toBe('MongoError');
                    expect(err.message).toBe('multi update only works with $ operators');

                    done();
                });
            });
        });
    });

    describe('has a function updateMany() which', function () {
        it('should update the document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.updateOne({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        repo.findOne({userName: 'bob'}, function (err, res1) {
                            expect(res1).toBeDefined();
                            expect(res1.userName).toBe('bob');
                            expect(res1.lastName).toBe('Norris');
                            expect(res1.email).toBe('chuck@norris.com');

                            done();
                        });
                    });
                });
            });
        });

        it('should update no document when the query matches no document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.updateOne({userName: 'chuck1'}, {userName: 'bob'}, function (err, res) {
                        expect(res).toBeDefined();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(0);

                        done();
                    });
                });
            });
        });

        it('should throw an exception when the number of params is less than 3', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.updateOne(1, 2);
            }).toThrow();

            expect(function () {
                return repo.updateOne(1);
            }).toThrow();

            expect(function () {
                return repo.updateOne();
            }).toThrow();
        });

        it('should update the document of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.updateOne({userName: 'chuck'}, {'$set': {userName: 'bob'}}, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update the document of the collection and has no options as parameter', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck',
                email: 'chuck@norris.com',
                birthdate: '1973-06-01T15:49:00.000Z',
                age: 44
            };

            repo.insert(user1, function () {
                repo.updateOne({userName: 'chuck'}, {'$set': {userName: 'bob'}}, null, function (err, res) {
                    expect(res).toBeDefined();
                    expect(res.result.ok).toBe(1);
                    expect(res.result.n).toBe(1);

                    repo.findOne({userName: 'bob'}, function (err, res1) {
                        expect(res1).toBeDefined();
                        expect(res1.userName).toBe('bob');
                        expect(res1.lastName).toBe('Norris');
                        expect(res1.email).toBe('chuck@norris.com');

                        done();
                    });
                });

            });
        });

        it('should update only one document of the collection', function (done) {
            var repo = userRepo(db.collection('users'));
            var user1 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck'
            };

            var user2 = {
                firstName: 'Chuck',
                lastName: 'Norris',
                userName: 'chuck2'
            };

            repo.insert(user1, function () {
                repo.insert(user2, function () {
                    repo.count({firstName: 'Chuck'}, function (err, res) {
                        expect(res).toBe(2);

                        repo.updateOne({firstName: 'Chuck'}, {'$set': {lastName: 'bob'}}, function (err, res) {
                            expect(res).toBeDefined();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(1);

                            repo.count({lastName: 'Norris'}, function (err, res) {
                                expect(res).toBe(1);
                                repo.count({lastName: 'bob'}, function (err, res) {
                                    expect(res).toBe(1);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    describe('has a function remove() which', function () {
        it('should remove the document of the collection with a query', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.insert({userName: 'troll'}, function () {
                        repo.remove({userName: 'chuck'}, function (err, res) {
                            expect(err).toBeNull();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(1);

                            repo.count(function (err, res1) {
                                expect(res1).toBe(2);

                                repo.remove({userName: 'wayne'}, {w: 1}, function (err, res2) {
                                    expect(res2).toBeDefined();
                                    expect(res2.result.ok).toBe(1);
                                    expect(res2.result.n).toBe(1);

                                    repo.count(function (err, res3) {
                                        expect(res3).toBe(1);

                                        repo.remove({userName: 'troll'});

                                        setTimeout(function () {
                                            repo.count(function (err, res) {
                                                expect(res).toBe(0);

                                                done();
                                            });
                                        }, 100);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should remove all documents of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove();

                    setTimeout(function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(0);

                            done();
                        });
                    }, 100);
                });
            });
        });

        it('should remove all documents of the collection with options', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove({w: 1});

                    setTimeout(function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(0);

                            done();
                        });
                    }, 100);
                });
            });
        });

        it('should remove all documents of the collection with callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove(function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(0);

                            done();
                        });
                    });
                });
            });
        });

        it('should remove all documents of the collection with callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove({w: 1}, function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(0);

                            done();
                        });
                    });
                });
            });
        });

        it('should return an error callback when the param "query" is not of type object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove(null, function (err, res) {
                        expect(err).toBeDefined();
                        expect(err instanceof TypeError).toBeTruthy();
                        expect(err.message).toBe('Param "query" is of type null! Type object expected');
                        expect(res).toBeUndefined();

                        done();
                    });
                });
            });
        });

        it('should return an error callback when the param "options" is not of type object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.remove({}, 33, function (err, res) {
                        expect(err).toBeDefined();
                        expect(err instanceof TypeError).toBeTruthy();
                        expect(err.message).toBe('Param "options" is of type number! Type object expected');
                        expect(res).toBeUndefined();

                        done();
                    });
                });
            });
        });

        it('should throw an exception when params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.remove(1, 2);
            }).toThrow();
            expect(function () {
                return repo.remove(1, 2, 3);
            }).toThrow();
            expect(function () {
                return repo.remove({}, 1);
            }).toThrow();
            expect(function () {
                return repo.remove({}, 1, 2);
            }).toThrow();
            expect(function () {
                return repo.remove({}, {}, 2);
            }).toThrow();
        });
    });

    describe('has a function deleteMany() which', function () {
        it('should remove the document of the collection with a query', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.insert({userName: 'troll'}, function () {
                        repo.deleteMany({userName: 'chuck'}, function (err, res) {
                            expect(err).toBeNull();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(1);

                            repo.count(function (err, res1) {
                                expect(res1).toBe(2);

                                repo.deleteMany({userName: 'wayne'}, {w: 1}, function (err, res2) {
                                    expect(res2).toBeDefined();
                                    expect(res2.result.ok).toBe(1);
                                    expect(res2.result.n).toBe(1);

                                    repo.count(function (err, res3) {
                                        expect(res3).toBe(1);

                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should remove all documents of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany(function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should remove no documents when the filter does not match', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany({w: 1}, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(0);

                        done();
                    });
                });
            });
        });

        it('should remove all documents of the collection with callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany(function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(0);

                            done();
                        });
                    });
                });
            });
        });

        it('should delete all documents when the filter is null', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany(null, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should delete all documents when the filter is no object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany(55, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should delete all documents when the options are no object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteMany(55, 66, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(2);

                        done();
                    });
                });
            });
        });

        it('should throw an exception when params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.deleteMany();
            }).toThrow();
            expect(function () {
                return repo.deleteMany(1, 2, 3);
            }).toThrow();
            expect(function () {
                return repo.deleteMany(1, 2);
            }).toThrow();
            expect(function () {
                return repo.deleteMany({});
            }).toThrow();
            expect(function () {
                return repo.deleteMany({}, {});
            }).toThrow();
        });
    });

    describe('has a function deleteOne() which', function () {
        it('should remove the document of the collection with a query', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.insert({userName: 'troll'}, function () {
                        repo.deleteOne({userName: 'chuck'}, function (err, res) {
                            expect(err).toBeNull();
                            expect(res.result.ok).toBe(1);
                            expect(res.result.n).toBe(1);

                            repo.count(function (err, res1) {
                                expect(res1).toBe(2);

                                repo.deleteOne({userName: 'wayne'}, {w: 1}, function (err, res2) {
                                    expect(res2).toBeDefined();
                                    expect(res2.result.ok).toBe(1);
                                    expect(res2.result.n).toBe(1);

                                    repo.count(function (err, res3) {
                                        expect(res3).toBe(1);

                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

        it('should remove one document of the collection', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne(function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should remove no documents when the filter does not match', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne({w: 1}, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(0);

                        done();
                    });
                });
            });
        });

        it('should remove one document of the collection with callback', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne(function () {
                        repo.count(function (err, res) {
                            expect(res).toBe(1);

                            done();
                        });
                    });
                });
            });
        });

        it('should delete one document when the filter is null', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne(null, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should delete one document when the filter is no object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne(55, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should delete one document when the options are no object', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne'}, function () {
                    repo.deleteOne(55, 66, function (err, res) {
                        expect(err).toBeNull();
                        expect(res.result.ok).toBe(1);
                        expect(res.result.n).toBe(1);

                        done();
                    });
                });
            });
        });

        it('should throw an exception when params are of wrong type', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.deleteOne();
            }).toThrow();
            expect(function () {
                return repo.deleteOne(1, 2, 3);
            }).toThrow();
            expect(function () {
                return repo.deleteOne(1, 2);
            }).toThrow();
            expect(function () {
                return repo.deleteOne({});
            }).toThrow();
            expect(function () {
                return repo.deleteOne({}, {});
            }).toThrow();
        });
    });

    describe('has a function aggregate() which', function () {
        it('should return an error callback when the param "pipeline" is not of type array', function (done) {
            var repo = sut(db.collection('users'));

            repo.aggregate('', {}, function (err, res) {
                expect(err).toBeDefined();
                expect(err instanceof TypeError).toBeTruthy();
                expect(err.message).toBe('Param "pipeline" is of type string! Type array expected');
                expect(res).toBeUndefined();

                done();
            });
        });

        it('should throw an exception when the number of params is less than 2', function () {
            var repo = sut(db.collection('users'));

            expect(function () {
                return repo.aggregate(1);
            }).toThrow();
        });

        it('should execute the aggregation pipeline', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 20}, function () {
                    repo.insert({userName: 'hans', age: 30}, function () {
                        repo.aggregate(pipeline, {}, function (err, res) {
                            expect(err).toBeNull();
                            expect(res).toBeDefined();
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(2);
                            expect(res).toEqual([
                                {_id: {age: 30}, count: 1},
                                {_id: {age: 20}, count: 2}
                            ]);

                            done();
                        });
                    });
                });
            });
        });

        it('should execute the aggregation pipeline and set options to empty object when number of params is 2', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 20}, function () {
                    repo.insert({userName: 'hans', age: 30}, function () {
                        repo.aggregate(pipeline, function (err, res) {
                            expect(err).toBeNull();
                            expect(res).toBeDefined();
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(2);
                            expect(res).toEqual([
                                {_id: {age: 30}, count: 1},
                                {_id: {age: 20}, count: 2}
                            ]);

                            done();
                        });
                    });
                });
            });
        });

        it('should execute the aggregation pipeline and set options to empty object when options are empty', function (done) {
            var repo = sut(db.collection('users'));

            repo.insert(user, function () {
                repo.insert({userName: 'wayne', age: 20}, function () {
                    repo.insert({userName: 'hans', age: 30}, function () {
                        repo.aggregate(pipeline, null, function (err, res) {
                            expect(err).toBeNull();
                            expect(res).toBeDefined();
                            expect(Array.isArray(res)).toBeTruthy();
                            expect(res.length).toBe(2);
                            expect(res).toEqual([
                                {_id: {age: 30}, count: 1},
                                {_id: {age: 20}, count: 2}
                            ]);

                            done();
                        });
                    });
                });
            });
        });
    });
});
