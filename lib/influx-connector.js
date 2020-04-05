"use strict";

var influx = require("influx");
var debug = require("debug")("connector:influxdb");
var schedule = require("node-schedule");

var async = require("async");
var parse = require("parse-duration");

/**
 * INITIALIZATION
 */

exports.initialize = function initializeDataSource(dataSource, callback) {
  var settings = dataSource.settings || {};

  dataSource.connector = new InfluxDBConnector(
    settings,
    dataSource["modelBuilder"]["models"]
  );

  if (dataSource.connector.settings.buffer) {
    var j = schedule.scheduleJob({ rule: "*/1 * * * * *" }, function () {
      var now = new Date().getTime();
      if (
        now >= dataSource.connector.startTime &&
        now < dataSource.connector.endTime &&
        dataSource.connector.buffer.length > 0
      ) {
        dataSource.connector.clearBuffer(function () {});
      }
    });
  }
  callback && process.nextTick(callback);
};

function InfluxDBConnector(settings, models) {
  var self = this;

  self.settings = settings;

  self.clusterHosts = self.settings.hosts || [
    {
      host: self.settings.host || "localhost",
      port: self.settings.port || 8086,
      protocol: self.settings.protocol || "http",
    },
  ];

  self.settings.bufferTime = self.settings.bufferTime || 1000;
  self.settings.bufferSize = self.settings.bufferSize || 1000;

  self.buffer = [];

  self.lastWrite = new Date().getTime();
  self.startTime = new Date(Date.now());
  self.endTime = new Date(Date.now());

  self.models = models;

  self.client = new influx.InfluxDB({
    //  cluster configuration
    hosts: self.clusterHosts,
    // or single-host configuration
    username: self.settings.username,
    password: self.settings.password,
    database: self.settings.database,

    failoverTimeout: self.settings.failoverTimeout || 60000,
    maxRetries: self.settings.maxRetries || 3,
    timePrecision: self.settings.timePrecision || "ms",
  });
}

exports.InfluxDBConnector = InfluxDBConnector;

InfluxDBConnector.prototype.connect = function (cb) {
  debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
  debug("this.clusterHosts ", this.client);
  debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
};

InfluxDBConnector.prototype.disconnect = function (cb) {};

InfluxDBConnector.prototype.save = function (model, data, cb) {
  this.bufferData(model, data, function (err) {
    if (err) {
      cb(err, null);
    } else {
      cb(null, data);
    }
  });
};

InfluxDBConnector.prototype.parseCQFunction = function (f) {
  f = f.toLowerCase();
  var functionMap = {
    count: "COUNT( )",
    countdistinct: "COUNT(DISTINCT( ))",
    distinct: "DISTINCT( )",
    // integral: 'INTEGRAL( )',
    mean: "MEAN( )",
    median: "MEDIAN( )",
    mode: "MODE( )",
    spread: "SPREAD( )",
    stddev: "STDDEV( )",
    sum: "SUM( )",
    bottom: "BOTTOM( )",
    first: "FIRST( )",
    last: "LAST( )",
    max: "MAX( )",
    min: "MIN( )",
    // percentile: 'PERCENTILE( )',
    // sample: 'SAMPLE( )',
    // top: 'TOP( )',
    // ceiling: 'CEILING(', // Ceiling has not been implemented yet
    cumulativesum: "CUMULATIVE_SUM( )",
    // derivative: 'DERIVATIVE( )',
    // difference: 'DIFFERENCE( )',
    // elapsed: 'ELAPSED( )',
    // floor: 'FLOOR(', // Floor has not been implemented yet
    // movingaverage: 'MOVING_AVERAGE( )',
    // nonnegativederivative: 'NON_NEGATIVE_DERIVATIVE( )',
    // nonnegativedifference: 'NON_NEGATIVE_DIFFERENCE( )'
  };

  if (functionMap.hasOwnProperty(f)) {
    return functionMap[f];
  } else {
    throw 'function "' + f + '" cannot be parsed';
  }
};

InfluxDBConnector.prototype.sortDurations = function (durations) {
  var sortedDurations = [];
  durations.forEach(function (duration) {
    if (duration !== "0s") {
      if (sortedDurations.length > 0) {
        for (var i = 0; i < sortedDurations.length; i++) {
          if (parse(duration) > parse(sortedDurations[i])) {
            sortedDurations.splice(i + 1, 0, duration);
            break;
          } else if (i === sortedDurations.length - 1) {
            sortedDurations.splice(i, 0, duration);
            break;
          } else if (i === 0) {
            sortedDurations.splice(i, 0, duration);
            break;
          }
        }
      } else {
        sortedDurations.push(duration);
      }
    }
  });
  sortedDurations.push("0s");
  return sortedDurations;
};

InfluxDBConnector.prototype.getMinimumDuration = function (models) {
  var minDuration;
  var durations = [];
  Object.keys(models).forEach(function (modelName) {
    var model = models[modelName];
    if (model && model.settings && model.settings.downSampling) {
      var dsRules = model.settings.downSampling;
      dsRules.forEach(function (dsRule) {
        durations.push(dsRule.duration);
      });
    }
  });

  durations.forEach(function (duration) {
    var parsedDuration = parse(duration);
    if (
      !minDuration ||
      (parsedDuration < parse(minDuration) && duration !== "0s")
    ) {
      minDuration = duration;
    }
  });
  if (minDuration) {
    return minDuration;
  } else {
    return "0s";
  }
};

/**
 * WRITE DATA
 */

InfluxDBConnector.prototype.create = function all(model, data, cb) {
  this.bufferData(model, data, function (err) {
    if (err) {
      cb(err, null);
    } else {
      cb(null, data);
    }
  });
};

InfluxDBConnector.prototype.bufferData = function (model, data, cb) {
  var self = this;

  // Obtain the model's schema
  var schema = self.models[model]["definition"]["properties"];
  var points = {
    measurement: model,
    tags: {},
    fields: {},
    timestamp: null,
  };

  // Format the data in the schema
  for (var d in data) {
    if (data[d] != null) {
      if (schema[d]["schema"] == "tag") {
        points["tags"][d] = data[d];
      } else if (schema[d]["schema"] == "field") {
        points["fields"][d] = data[d];
      } else if (schema[d]["schema"] == "timestamp") {
        points["timestamp"] = data[d];
      } else {
        points["tags"][d] = data[d];
      }
    }
  }

  // Add the data to the buffer
  this.buffer.push(points);

  // Push forward the time for scheduled buffer clearing
  this.startTime = new Date(Date.now() + this.settings.bufferTime);
  this.endTime = new Date(this.startTime.getTime() + 1001);

  // Determine whether to write the buffer to the database
  if (
    this.settings.buffer &&
    new Date().getTime() < this.lastWrite + this.settings.bufferTime &&
    this.buffer.length < this.settings.bufferTime
  ) {
    cb(null, data);
  } else {
    this.clearBuffer(function (err, response) {
      if (err) {
        cb(err, null);
      } else {
        cb(null, data);
      }
    });
  }
};

InfluxDBConnector.prototype.clearBuffer = function (cb) {
  var connector = this;
  this.client
    .writePoints(this.buffer, {
      precision: this.settings.timePrecision,
      // rp: this.settings.defaultRp,
      database: this.settings.database,
    })
    .then(function () {
      connector.buffer = [];
      connector.lastWrite = new Date(Date.now()).getTime();
      cb(null, "cleared");
    })
    .catch(function (err) {
      cb(err, null);
    });
};

/**
 * READ DATA
 */

/**
 * Find an instance of a given model/id
 * @param {string} model The model name
 * @param {*} id The id value
 * @param {function} [callback] The callback function
 */
InfluxDBConnector.prototype.find = function find(model, id, callback) {
  callback("Not Implemented");
};

InfluxDBConnector.prototype.buildQuery = function (model, filter, rp, cb) {
  var self = this;

  if (filter) {
    var db = self.settings.database;
    if (filter.where && filter.where.appId) {
      db = filter.where.appId;
      delete filter.where.appId;
    }

    var query = "SELECT ";

    if (filter.select) {
      if (Array.isArray(filter.select)) {
        filter.select.forEach(function (selector, index) {
          var [err, q, aggregator, selector, transformer] = self.buildSelect(
            selector
          );
          if (err) {
            cb(err);
            return;
          } else {
            if (index > 0) {
              query += ", ";
            }
            query += q;
          }
        });
      } else {
        var [err, q, aggregator, selector, transformer] = self.buildSelect(
          filter.select
        );
        if (err) {
          cb(err);
          return;
        } else {
          query += q;
        }
      }
    } else {
      var selectObj = {
        transformer: filter.transformer,
        selector: filter.selector,
        aggregator: filter.aggregator,
        property: filter.property,
        aggregatorParameter: filter.aggregatorParameter,
        selectorParameter: filter.selectorParameter,
        transformerParameter: filter.transformerParameter,
        as: filter.as,
      };

      var [err, q, aggregator, selector, transformer] = self.buildSelect(
        selectObj
      );
      if (err) {
        cb(err);
        return;
      } else {
        query += q;
      }
    }

    if (filter.from) {
      self.buildQuery(model, filter.from, "", function (err, subQ) {
        if (err) {
          cb(err);
          return;
        } else {
          query += " FROM (" + subQ + ")";

          if (filter.where) {
            query += " WHERE " + self.buildWhere(filter.where, model);
          }

          if (filter.group) {
            if (filter.group.indexOf("time") !== -1) {
              if (
                (aggregator !== null ||
                  selector !== null ||
                  transformer !== null) &&
                filter.where &&
                !(transformer === "cumulativesum" && aggregator === null) &&
                !(transformer === "derivative" && aggregator === null) &&
                !(transformer === "difference" && aggregator === null) &&
                !(transformer === "movingaverage" && aggregator === null) &&
                !(
                  transformer === "nonnegativederivative" && aggregator === null
                ) &&
                !(
                  transformer === "nonnegativedifference" && aggregator === null
                )
              ) {
                query = query + " GROUP BY " + filter.group;

                if (filter.fill && aggregator !== "integral") {
                  // if (filter.fill) {
                  query = query + " fill(" + filter.fill + ")";
                }
              } else {
                cb(
                  new Error(
                    "This query does not allow the response to be grouped by time"
                  )
                );
              }
            } else {
              query = query + " GROUP BY " + filter.group;
            }
          }

          if (filter.order) {
            query += " ORDER BY " + filter.order;
          }

          if (filter.limit) {
            query += " LIMIT " + filter.limit;
          } else {
            query += " LIMIT " + 100000;
          }

          if (filter.offset) {
            query += " OFFSET " + filter.offset;
          }

          if (filter.tz) {
            query += " tz(" + filter.tz + ")";
          }

          cb(null, query);
        }
      });
    } else {
      var rpName = self.retentionPolicies[rp];
      if (rpName) {
        query += ' FROM "' + rpName + '"."' + model + '"';
      } else {
        query += ' FROM "' + model + '"';
      }

      if (filter.where) {
        query += " WHERE " + self.buildWhere(filter.where, model);
      }

      if (filter.group) {
        //  console.log("buildQuery 4 : ", filter.group)

        if (filter.group.indexOf("time") !== -1) {
          if (
            (aggregator !== null ||
              selector !== null ||
              transformer !== null) &&
            filter.where &&
            !(transformer === "cumulativesum" && aggregator === null) &&
            !(transformer === "derivative" && aggregator === null) &&
            !(transformer === "difference" && aggregator === null) &&
            !(transformer === "movingaverage" && aggregator === null) &&
            !(transformer === "nonnegativederivative" && aggregator === null) &&
            !(transformer === "nonnegativedifference" && aggregator === null)
          ) {
            query = query + " GROUP BY " + filter.group;

            if (filter.fill && aggregator !== "integral") {
              // if (filter.fill) {
              query = query + " fill(" + filter.fill + ")";
            }
          } else {
            cb(
              new Error(
                "This query does not allow the response to be grouped by time"
              )
            );
          }
        } else {
          query = query + " GROUP BY " + filter.group;
        }
      }

      if (filter.order) {
        query += " ORDER BY " + filter.order;
      }

      if (filter.limit) {
        query += " LIMIT " + filter.limit;
      } else {
        query += " LIMIT " + 100000;
      }

      if (filter.offset) {
        query += " OFFSET " + filter.offset;
      }

      if (filter.tz) {
        query += " tz(" + filter.tz + ")";
      }

      cb(null, query);
    }
  } else {
    var rpName = self.retentionPolicies[rp];
    query = 'SELECT * FROM "' + rpName + '"."' + model + '"';
    cb(null, query);
  }
};

var operators = {
  eq: "=",
  lt: "<",
  gt: ">",
  gte: ">=",
  lte: "<=",
  neq: "!=",
};

InfluxDBConnector.prototype.buildWhere = function (obj, model) {
  var self = this;
  var schema = self.models[model]["definition"]["properties"];
  return (
    "(" +
    Object.keys(obj)
      .map(function (k) {
        var whereClause = [];
        if (k === "and" || k === "or") {
          if (Array.isArray(obj[k])) {
            return (
              "(" +
              obj[k]
                .map(function (condObj) {
                  return self.buildWhere(condObj, model);
                })
                .join(" " + k + " ") +
              ")"
            );
          }
        } else {
          if (typeof obj[k] == "object") {
            if (obj[k] instanceof Date) {
              var val3 = obj[k];
              if (!isNaN(val3) && schema[k]["schema"] === "timestamp") {
                val3 = new Date(val3).toISOString();
                val3 = "'" + val3 + "'";
              } else {
                if (val3 !== true && val3 !== false) {
                  if (schema[k]["schema"] !== "field") {
                    val3 = "'" + val3 + "'";
                  }
                }
              }
              whereClause.push('"' + k + '"=' + val3);
            } else {
              for (var p in obj[k]) {
                if (operators[p]) {
                  var val = obj[k][p];
                  if (!isNaN(val) && schema[k]["schema"] === "timestamp") {
                    val = new Date(val).toISOString();
                    val = "'" + val + "'";
                  } else {
                    if (val !== true && val !== false) {
                      if (schema[k]["schema"] !== "field") {
                        val = "'" + val + "'";
                      }
                    }
                  }
                  whereClause.push('"' + k + '"' + operators[p] + val);
                } else if (p == "inq") {
                  var vals = obj[k][p];
                  var inqClause = [];
                  for (var val in vals) {
                    if (schema[k]["schema"] !== "field") {
                      inqClause.push(
                        '"' + k + '"' + "=" + "'" + vals[val] + "'"
                      );
                    } else {
                      inqClause.push('"' + k + '"' + "=" + vals[val]);
                    }
                  }
                  whereClause.push("(" + inqClause.join(" OR ") + ")");
                }
              }
            }
          } else {
            var val2 = obj[k];
            if (!isNaN(val2) && schema[k]["schema"] === "timestamp") {
              val2 = new Date(val2).toISOString();
              val2 = "'" + val2 + "'";
            } else {
              if (val2 !== true && val2 !== false) {
                if (schema[k]["schema"] !== "field") {
                  val2 = "'" + val2 + "'";
                }
              }
            }
            whereClause.push('"' + k + '"=' + val2);
          }
          return "(" + whereClause.join(" AND ") + ")";
        }
      })
      .join(" AND ") +
    ")"
  );
};

// TODO Migrate build select to this function (starting with advanced)
InfluxDBConnector.prototype.buildSelect = function (obj) {
  var subQuery = "";

  var transformer = null;
  var selector = null;
  var aggregator = null;

  if (obj.transformer) {
    transformer = obj.transformer.toLowerCase();
    var transformerMap = {
      // ceiling: 'CEILING(', // Ceiling has not been implemented yet
      cumulativesum: "CUMULATIVE_SUM(",
      derivative: "DERIVATIVE(",
      difference: "DIFFERENCE(",
      elapsed: "ELAPSED(",
      // floor: 'FLOOR(', // Floor has not been implemented yet
      // histogram: 'HISTOGRAM(', // Histogram has not been implemented yet
      movingaverage: "MOVING_AVERAGE(",
      nonnegativederivative: "NON_NEGATIVE_DERIVATIVE(",
      nonnegativedifference: "NON_NEGATIVE_DIFFERENCE(",
    };
    if (transformerMap.hasOwnProperty(transformer)) {
      subQuery = subQuery + transformerMap[transformer];
    } else {
      // rpCb(new Error(transformer + ' is currently not supported as a transformer'), null)
      return [
        new Error(transformer + " is currently not supported as a transformer"),
        null,
        null,
        null,
        null,
      ];
    }
  }

  if (obj.selector) {
    if (transformer === null) {
      selector = obj.selector.toLowerCase();
      var selectorMap = {
        bottom: "BOTTOM(",
        first: "FIRST(",
        last: "LAST(",
        max: "MAX(",
        min: "MIN(",
        percentile: "PERCENTILE(",
        sample: "SAMPLE(",
        top: "TOP(",
      };
      if (selectorMap.hasOwnProperty(selector)) {
        subQuery = subQuery + selectorMap[selector];
      } else {
        // rpCb(new Error(selector + ' is currently not supported as a selector'), null);
        return [
          new Error(selector + " is currently not supported as a selector"),
          null,
          null,
          null,
          null,
        ];
      }
    } else {
      // rpCb(new Error('A selector cannot be combined with a transformer'), null);
      return [
        new Error("A selector cannot be combined with a transformer"),
        null,
        null,
        null,
        null,
      ];
    }
  }

  if (obj.aggregator) {
    if (selector === null) {
      aggregator = obj.aggregator.toLowerCase();
      var aggregatorMap = {
        count: "COUNT(",
        countdistinct: "COUNT(DISTINCT(",
        distinct: "DISTINCT(",
        integral: "INTEGRAL(",
        mean: "MEAN(",
        median: "MEDIAN(",
        mode: "MODE(",
        spread: "SPREAD(",
        stddev: "STDDEV(",
        sum: "SUM(",
      };
      if (aggregatorMap.hasOwnProperty(aggregator)) {
        subQuery = subQuery + aggregatorMap[aggregator];
      } else {
        // rpCb(new Error(aggregator + ' is currently not supported as an aggregator'), null);
        return [
          new Error(
            aggregator + " is currently not supported as an aggregator"
          ),
          null,
          null,
          null,
          null,
        ];
      }
    } else {
      // rpCb(new Error('An aggregator cannot be combined with a selector'), null);
      return [
        new Error("An aggregator cannot be combined with a selector"),
        null,
        null,
        null,
        null,
      ];
    }
  } else {
    if (transformer === "movingaverage") {
      // rpCb(new Error(transformer + ' needs to be combined with an aggregator'), null);
      return [
        new Error(transformer + " needs to be combined with an aggregator"),
        null,
        null,
        null,
        null,
      ];
    }
  }

  var property = "*";
  if (obj.property) {
    property = obj.property.toString();
  } else {
    if (aggregator === "countdistinct") {
      // rpCb(new Error(aggregator + ' requires the specification of a property'),null);
      return [
        new Error(aggregator + " requires the specification of a property"),
        null,
        null,
        null,
        null,
      ];
    }
    if (selector === "bottom" || selector === "top") {
      return [
        new Error(selector + " requires the specification of a property"),
        null,
        null,
        null,
        null,
      ];
      // return;
    }
  }
  subQuery = subQuery + property;

  if (obj.aggregatorParameter) {
    if (aggregator === "integral") {
      subQuery = subQuery + "," + obj.aggregatorParameter;
    }
  }
  if (aggregator !== null) {
    subQuery = subQuery + ")";
  }
  if (aggregator === "countdistinct") {
    subQuery = subQuery + ")";
  }

  if (obj.selectorParameter) {
    if (
      selector === "bottom" ||
      selector === "percentile" ||
      selector === "sample" ||
      selector === "top"
    ) {
      subQuery = subQuery + "," + obj.selectorParameter;
    }
  } else {
    if (
      selector === "bottom" ||
      selector === "percentile" ||
      selector === "sample" ||
      selector === "top"
    ) {
      return [
        new Error(selector + " requires the specification of a parameter"),
        null,
      ];
      // return;
    }
  }
  if (selector !== null) {
    subQuery = subQuery + ")";
  }

  if (obj.transformerParameter) {
    if (
      transformer === "derivative" ||
      transformer === "elapsed" ||
      transformer === "movingaverage" ||
      transformer === "nonnegativederivative"
    ) {
      subQuery = subQuery + "," + obj.transformerParameter;
    }
  } else {
    if (transformer === "movingaverage") {
      return [
        new Error(transformer + " requires the specification of a parameter"),
        null,
        null,
        null,
        null,
      ];
      // return;
    }
  }
  if (transformer !== null) {
    subQuery = subQuery + ")";
  }

  if (obj.as) {
    subQuery += " AS " + obj.as;
  }

  return [null, subQuery, aggregator, selector, transformer];
};

InfluxDBConnector.prototype.getTimeLimits = function (filter) {
  var lowerLimitOperators = ["gte", "gt"];
  var upperLimitOperators = ["lte", "lt"];
  var lowerLimit;
  var upperLimit;
  if (filter && filter.where) {
    if (filter.where.and) {
      var timeFilters = filter.where.and.filter((f) => {
        return f.time !== undefined && f.time !== null;
      });
      for (var i = 0; i < timeFilters.length; i++) {
        var timeFilter = timeFilters[i];
        var tf = timeFilter.time;
        lowerLimitOperators.forEach(function (operator) {
          if (tf[operator] !== undefined) {
            lowerLimit = tf[operator];
          }
        });
        upperLimitOperators.forEach(function (operator) {
          if (tf[operator] !== undefined) {
            upperLimit = tf[operator];
          }
        });

        if (tf.eq !== undefined) {
          lowerLimit = tf.eq;
          upperLimit = tf.eq;
          break;
        } else if (tf instanceof Date) {
          lowerLimit = tf;
          upperLimit = tf;
          break;
        }
      }
    } else if (filter.where.time) {
      var tf = filter.where.time;
      lowerLimitOperators.forEach(function (operator) {
        if (tf[operator] !== undefined) {
          lowerLimit = tf[operator];
        }
      });
      upperLimitOperators.forEach(function (operator) {
        if (tf[operator] !== undefined) {
          upperLimit = tf[operator];
        }
      });

      if (tf.eq !== undefined) {
        lowerLimit = tf.eq;
        upperLimit = tf.eq;
      } else if (tf instanceof Date) {
        lowerLimit = tf;
        upperLimit = tf;
      }
    }
  }
  if (lowerLimit === undefined) {
    lowerLimit = new Date(0);
  }
  if (upperLimit === undefined) {
    upperLimit = new Date();
  }
  return [lowerLimit, upperLimit];
};

InfluxDBConnector.prototype.getConsideredRPs = function (filter) {
  var self = this;
  var timeLimits = self.getTimeLimits(filter);
  var lowerLimit = timeLimits[0].getTime();
  var upperLimit = timeLimits[1].getTime();

  var now = new Date().getTime();
  var limit = now;
  var rpsToBeConsidered = [];
  self
    .sortDurations(Object.keys(self.retentionPolicies))
    .forEach(function (duration) {
      var previousLimit = limit;
      if (duration !== "0s") {
        limit = now - parse(duration);
      } else {
        limit = 0;
      }

      var intervalInRP = upperLimit >= limit && lowerLimit <= previousLimit;
      if (intervalInRP) {
        rpsToBeConsidered.push(duration);
      }
    });
  return rpsToBeConsidered;
};

/**
 * Query all instances for a given model based on the filter
 * @param {string} model The model name
 * @param {object} filter The filter object
 * @param {function} [callback] The callback function
 */
InfluxDBConnector.prototype.all = function all(model, filter, callback) {
  var self = this;
  var consideredRPs = self.getConsideredRPs(filter);
  var readings = [];
  async.eachSeries(
    consideredRPs,
    function (rp, rpCb) {
      self.buildQuery(model, filter, rp, function (err, query) {
        //self.buildQuery(model, filter, function(err, query) {
        if (err) {
          callback(err, null);
        } else {
          self.client
            .query(query)
            .then(function (results) {
              results = results.slice(0, results.length);
              //  callback(null, results)
              readings.push(results);
              rpCb();
            })
            .catch(function (err) {
              //  callback(err, null)
              rpCb(err);
            });
        }
      });
    },
    function done(err) {
      if (err) {
        callback(err, null);
      } else {
        var response = [];
        for (var i = readings.length - 1; i >= 0; i--) {
          var rpReadings = readings[i];
          if (response.length == 0) {
            response = rpReadings;
          } else {
            var lastReading = response[response.length - 1];
            if (readings[i + 1].length > 1) {
              var secondToLastReading = response[response.length - 2];
            }
            for (var j = 0; j < rpReadings.length; j++) {
              var reading = rpReadings[j];
              if (readings[i + 1].length > 1) {
                if (
                  new Date(reading.time).getTime() >
                  new Date(lastReading.time).getTime() +
                    (new Date(lastReading.time).getTime() -
                      new Date(secondToLastReading.time).getTime())
                ) {
                  response.push(reading);
                }
              } else {
                if (reading.time > lastReading.time) {
                  response.push(reading);
                }
              }
            }
          }
        }
        callback(null, response);
      }
    }
  );
};

/**
 * Get types associated with the connector
 * @returns {String[]} The types for the connector
 */
InfluxDBConnector.prototype.getTypes = function () {
  return ["influxdb"];
};

// /**
//  * Update a point
//  * @param data - the new data to be placed in the point.
//  * @param point - The point object to be updated.
//  *
//  * example:
//  * updatePoint({"tag1":"US2341", "tag2":"Windows", "value":233}, point)
//  */
InfluxDBConnector.prototype.updatePoint = function updatePoint(
  model,
  attributes,
  instance
  // data,
  // point
) {
  var schema = self.models[model]["definition"]["properties"];
  var point = {
    measurement: model,
    tags: {},
    fields: {},
    timestamp: null,
    time: instance.time,
  };

  var data = {
    tags: {},
    fields: {},
    timestamp: null,
  };

  // Format the data in the schema
  for (var d in instance) {
    if (instance[d] != null) {
      if (schema[d]["schema"] == "tag") {
        point["tags"][d] = instance[d];
      } else if (schema[d]["schema"] == "field") {
        point["fields"][d] = instance[d];
      } else if (schema[d]["schema"] == "timestamp") {
        point["timestamp"] = instance[d];
      } else {
        point["tags"][d] = instance[d];
      }
    }
  }

  for (var d in attributes) {
    if (attributes[d] != null) {
      if (schema[d]["schema"] == "tag") {
        data["tags"][d] = attributes[d];
      } else if (schema[d]["schema"] == "field") {
        data["fields"][d] = attributes[d];
      } else if (schema[d]["schema"] == "timestamp") {
        data["timestamp"] = attributes[d];
      } else {
        data["tags"][d] = attributes[d];
      }
    }
  }

  //  var query = 'INSERT ' + point.measurement +',';
  var query = "INSERT " + model + ",";

  // Add all tags
  Object.keys(point.tags).forEach(function (tag) {
    var value = point.tags[tag];
    // Check if there is a updated value for the tag
    Object.keys(data.tags).forEach(function (newTag) {
      if (tag == newTag) value = data.tag[newTag];
    });
    query += tag + "=" + value;
  });

  // Add all values
  Object.keys(point.fields).forEach(function (field) {
    var value = point.fields[field];
    // Check if there is a updated value for the field
    Object.keys(data.fields).forEach(function (newField) {
      if (field == newField) value = data.fields[newField];
    });
    query += field + "=" + value;
  });

  query += " " + point.time;
  //  console.log("updatePoint : ", query);
  return query;

  // Execute the query
  // self.client.query(query).then(function(results) {
  //   console.log(results);
  // })
};
