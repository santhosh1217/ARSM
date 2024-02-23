var mongodb = require('./MongodDbUtil');
var config = require('../config/config.' + process.env.NODE_ENV);

function create(record, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(record, this.getCollectionName(), function (collName) 
    {
        var coll = db.collection(collName);
        coll.insert(record, function (err, result) 
        {
            if (!err) {
                callback(null, result.ops[0]);
            } else {
                callback(err, null);
            }
        });
    });
}

function createMany(records, callback) 
{
    var db = mongodb.getDb();

    getCollectionNameDyn((records && records.length) ? (records[0]) : null, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.insertMany(records, function (err, result) 
        {
            if (!err) {
                callback(null, result.ops[0]);
            } else {
                callback(err, null);
            }
        });
    });
}

function getAll(academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.find({}).toArray(function (err, result) {
            if (!err) {

                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getById(id, academicTerm, callback) {

    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    if (!mongodb.ObjectID.isValid(id)) {
        callback("Invalid id");
        return;
    }
    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.findOne({ _id: mongodb.ObjectID(id) }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getByIds(ids, academicTerm, callback) {
    if (!ids || !ids.length) {
        callback(null, []);
        return;
    }
    var arrayOfIds = [];
    ids.forEach(function (id) {
        arrayOfIds.push(mongodb.ObjectID(id));
    });

    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.find({ _id: { "$in": arrayOfIds } }).toArray(function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getOneByQuery(query, projection, callback) 
{
    var db = mongodb.getDb();
    var projectionObj = {};

    if (typeof projection == "function") {
        callback = projection;
        projection = null;
    }
    if (projection) {
        projection.forEach(function (p) {
            projectionObj[p] = 1;
        });
    }

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.findOne(query, projectionObj, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getByQuery(query, projection, callback) {
    if (typeof projection == "function") {
        callback = projection;
        projection = null;
    }

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        processQueryInCollection(collName, query, projection, callback);
    });
}

function getCountByQuery(query, callback) {
    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var db = mongodb.getDb();
        var coll = db.collection(collName);

        var cursor;
        cursor = coll.count(query, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function processQueryInCollection(collectionName, query, projection, callback) {
    var db = mongodb.getDb();
    var coll = db.collection(collectionName);

    var cursor;
    if (projection) {
        var projectionObj = {};
        if (projection instanceof Array) {
            projection.forEach(function (p) {
                projectionObj[p] = 1;
            });
        }
        else {
            projectionObj = projection;
        }
        cursor = coll.find(query, projectionObj);
    } else {
        cursor = coll.find(query);
    }

    cursor.toArray(function (err, result) {
        if (!err) {
            callback(null, result);
        } else {
            callback(err, null);
        }
    });
}

function getAndSortByQuery(query, projection, sortCriteria, callback) {
    if (typeof projection == "function") {
        callback = projection;
        projection = null;
    }
    if (typeof sortCriteria == "function") {
        callback = sortCriteria;
        sortCriteria = null;
    }

    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);

        var cursor;
        if (projection) {
            var projectionObj = {};
            projection.forEach(function (p) {
                projectionObj[p] = 1;
            });
            cursor = coll.find(query, projectionObj);
        } else {
            cursor = coll.find(query);

        }

        if (sortCriteria) {
            cursor.sort(sortCriteria);
        }

        cursor.toArray(function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });

    });
}

function update(query, detailsToUpdate, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update(query, { $set: detailsToUpdate }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function upsert(query, detailsToUpdate, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update(query, { $set: detailsToUpdate }, { multi: false, upsert: true }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

/**
 * deprecated
 */
function findAndModify(query, detailsToUpdate, callback) {
    var db = mongodb.getDb();
    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.findAndModify(query, [], { $set: detailsToUpdate }, { upsert: true, new: true }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function findOneByIdAndUpdate(id, detailsToUpdate, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.findOneAndUpdate({ _id: mongodb.ObjectID(id) }, { $set: detailsToUpdate }, { returnOriginal: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function updateToUnset(query, detailsToUpdate, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.updateMany(query, { $unset: detailsToUpdate }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function updateToUnsetById(id, detailsToUpdate, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update({ _id: mongodb.ObjectID(id) }, { $unset: detailsToUpdate }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function updateArrayById(id, elementsToPush, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update({ _id: mongodb.ObjectID(id) }, { $push: elementsToPush }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}


function updateArrayByQuery(query, elementsToPush, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);

        coll.updateMany(query, { $push: elementsToPush }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function removeItemInArrayByQuery(query, elementToDelete, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update(query, { $pull: elementToDelete }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function updateById(id, detailsToUpdate, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);

        var deletedId;
        if (detailsToUpdate._id) {
            deletedId = detailsToUpdate._id;
            delete detailsToUpdate._id;
        }

        coll.update({ _id: mongodb.ObjectID(id) }, { $set: detailsToUpdate }, { multi: false }, function (err, result) {
            if (deletedId) {
                detailsToUpdate._id = deletedId;
            }

            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function updateMany(query, detailsToUpdate, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.updateMany(query, { $set: detailsToUpdate }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function distinctByQuery(field, query, academicTerm, callback) {
    if (typeof query == "function") {
        callback = query;
        query = null;
        academicTerm = null;
    }

    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }

    var db = mongodb.getDb();

    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        if (!query)
            query = {};
        coll.distinct(field, query, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        })
    });
}

function remove(id, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.remove({ _id: mongodb.ObjectID(id) }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function removeByQuery(query, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.remove(query, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getIdFilter(entity) {
    return { _id: mongodb.ObjectID(entity._id) };
}

function removeItemInArrayById(id, elementToDelete, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.update({ _id: mongodb.ObjectID(id) }, { $pull: elementToDelete }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getMongoDb() {
    return mongodb;
}

function bulkWrite(bulk, academicTerm, callback) {
    if (typeof academicTerm == "function") {
        callback = academicTerm;
        academicTerm = null;
    }
    var db = mongodb.getDb();

    //passing null as first param will take current academic year
    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.bulkWrite(bulk, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function getDb() {
    return monogdb.getDb();
}

function getCollectionNameDyn(query, collectionName, cb) {
    var collName = null;
    if (config.collsWithAcademicTermId.includes(collectionName)) {
        if (!query || !query.academicYear || !query.academicSemester) {
            getCurrentAcademicYear(function (err, res) {
                if (!err) {
                    collName = collectionName + "_" + res.academicYear + "_" + res.academicSemester;
                    cb(collName);
                }
            })
        }
        else {
            collName = collectionName + "_" + query.academicYear + "_" + query.academicSemester;
            cb(collName);
        }
    }
    else {
        cb(collectionName);
    }
}

function getCurrentAcademicYear(cb) {
    var query = {
        'IsCurrentAcademicYear': true
    };
    var collectionName = "academicTerms";
    var db = mongodb.getDb();
    var coll = db.collection(collectionName);
    coll.findOne(query, function (err, result) {
        if (!err) {
            cb(null, result)
        } else {
            cb(err, null);
        }
    });
}

function checkIfCurrentAcademicTerm(q, contextCollName, cb) {
    if (config.collsIgnoredForHistData.includes(contextCollName)) {
        cb(null, { currentAcademicTerm: true })
        return;
    }
    var collectionName = "academicTerms";
    var query = {
        "academicYear": q["academicYear"],
        "academicSemester": q["academicSemester"]
    }
    var db = mongodb.getDb();
    var coll = db.collection(collectionName);
    coll.findOne(query, function (err, result) {
        if (!err) {
            cb(null, { currentAcademicTerm: result.IsCurrentAcademicYear })
        } else {
            cb(err, null);
        }
    });
}

function getByAggregation(pipeline, options, academicTerm, callback) {
    if (!options) {
        options = {};
    }
    var db = mongodb.getDb();

    getCollectionNameDyn(academicTerm, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.aggregate(pipeline, options, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        })
    });
}

function increment(query, incObj, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.updateOne(query, { $inc: incObj }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

function removeItemsInArrayByQuery(query, elementToDelete, callback) {
    var db = mongodb.getDb();

    getCollectionNameDyn(query, this.getCollectionName(), function (collName) {
        var coll = db.collection(collName);
        coll.updateMany(query, { $pull: elementToDelete }, { multi: false }, function (err, result) {
            if (!err) {
                callback(null, result);
            } else {
                callback(err, null);
            }
        });
    });
}

module.exports = function BaseDao(collectionName) {
    return {
        create: create,
        createMany: createMany,
        getAll: getAll,
        getById: getById,
        getByIds: getByIds,
        getByQuery: getByQuery,
        getCountByQuery: getCountByQuery,
        getOneByQuery: getOneByQuery,
        getAndSortByQuery: getAndSortByQuery,
        update: update,
        upsert: upsert,
        findAndModify: findAndModify,
        findOneByIdAndUpdate: findOneByIdAndUpdate,
        updateById: updateById,
        updateMany: updateMany,
        updateToUnset: updateToUnset,
        updateToUnsetById: updateToUnsetById,
        updateArrayById: updateArrayById,
        updateArrayByQuery: updateArrayByQuery,
        removeItemInArrayByQuery: removeItemInArrayByQuery,
        distinctByQuery: distinctByQuery,
        remove: remove,
        removeByQuery: removeByQuery,
        removeItemInArrayById: removeItemInArrayById,
        bulkWrite: bulkWrite,
        getIdFilter: getIdFilter,
        getDb: getDb,
        getByAggregation: getByAggregation,
        increment: increment,
        removeItemsInArrayByQuery: removeItemsInArrayByQuery,
        getCollectionName: function () {
            return collectionName;
        },
        getCollectionNameDyn: getCollectionNameDyn
    };
};