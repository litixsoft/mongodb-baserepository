'use strict';

var lxDb = require(require('path').join(process.cwd(), 'lib', 'mongodb-baserepository'));

module.exports = function (collection) {
    function schema () {
        return {
            properties: {
                _id: {
                    type: 'string',
                    required: false,
                    format: 'mongo-id',
                    key: true
                },
                chief_id: {
                    type: ['string', 'mongoId'],
                    required: false,
                    format: 'mongo-id'
                },
                birthdate: {
                    type: 'string',
                    required: true,
                    format: 'date-time'
                },
                email: {
                    type: 'string',
                    required: true,
                    format: 'email'
                },
                firstName: {
                    type: 'string',
                    required: true
                },
                lastName: {
                    type: 'string',
                    required: true
                },
                userName: {
                    type: 'string',
                    required: true,
                    sort: 1,
                    unique: true
                },
                age: {
                    type: 'integer',
                    required: false
                },
                indexProp: {
                    type: 'string',
                    index: 1
                },
                uniqueProp: {
                    type: 'string'
                },
                c: {
                    type: 'object',
                    properties: {
                        d: {
                            type: 'string',
                            index: 1
                        }
                    }
                },
                i: {
                    type: 'object',
                    properties: {
                        ii: {
                            type: 'object',
                            properties: {
                                name: {
                                    type: 'string'
                                },
                                manager_id: {
                                    type: 'string',
                                    format: 'mongo-id'
                                },
                                iii: {
                                    type: 'object',
                                    properties: {
                                        iiii: {
                                            type: 'object',
                                            properties: {
                                                name: {
                                                    type: 'string',
                                                    index: 1
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                a: {
                    type: 'array',
                    items: {
                        aa: {
                            type: 'array',
                            items: {
                                name: {
                                    type: 'string'
                                },
                                aaa: {
                                    type: 'array',
                                    items: {
                                        aaaa: {
                                            type: 'array',
                                            items: {
                                                name: {
                                                    type: 'string',
                                                    index: -1
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    var baseRepo = lxDb(collection, schema);

    // validators
    baseRepo.checkUserName = function (userName, cb) {
        collection.findOne({userName: userName}, function (err, res) {
            if (err) {
                cb(err);
            } else if (res) {
                cb(null, {
                    valid: false, errors: [
                        {
                            attribute: 'checkUserName',
                            property: 'userName', expected: false, actual: true,
                            message: 'userName already exists'
                        }
                    ]
                });
            } else {
                cb(null, {valid: true});
            }
        });
    };

    baseRepo.checkUserEmail = function (email, cb) {
        collection.findOne({email: email}, function (err, res) {
            if (err) {
                cb(err);
            } else if (res) {
                cb(null, {
                    valid: false, errors: [
                        {
                            attribute: 'checkUserEmail',
                            property: 'email', expected: false, actual: true,
                            message: 'email already exists'
                        }
                    ]
                });
            } else {
                cb(null, {valid: true});
            }
        });
    };

    baseRepo.convert = function (doc) {
        if (doc.hasOwnProperty('birthdate') && typeof doc.birthdate === 'string') {
            doc.birthdate = new Date(doc.birthdate);
        }

        return doc;
    };

    return baseRepo;
};
