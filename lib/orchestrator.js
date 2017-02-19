const net = require('net');
const Protocol = require('kafka-wire-protocol').Protocol;
const defs = require('kafka-wire-protocol/lib/protocol/definitions.js');
const apiTemlplates = require('kafka-wire-protocol/lib/protocol/binaryTemplates/api');
const ApiError = require('./apiError');

module.exports = class Orchestrator {

  constructor(clientId, bsHost, bsPort) {
    this.clientId = clientId;
    this.kp = new Protocol({
      clientId: this.clientId
    });
    this.bsHost = bsHost;
    this.bsPort = bsPort;
    this.utilBuffer = Buffer.alloc(1024);

    this.topics = {};
    this.brokers = null;
    this.versions = null;
  }

  initApiVersions(cb) {
    this.apiVersions((err, result) => {
      if(err) return cb(err);
      this.versions = result.apiVersions.reduce((keyedVersions, api) => {
        if(!defs.API_KEYS[api.apiKey]) return keyedVersions;
        let name = defs.API_KEYS[api.apiKey];
        var version = 0;
        if(apiTemlplates[name] && apiTemlplates[name].versions) {
          let maxLibVer = apiTemlplates[name].versions.length - 1;
          if(api.maxVersion <= maxLibVer) {
            version = api.maxVersion
          } else if(api.minVersion > maxLibVer) {
            return cb(new Error(`Minimum required version of api is ${api.minVersion}, library supports up to ${maxLibVer}.`));
          }
        } else if(api.minVersion > 0) {
          return cb(new Error(`Minimum required version of api is ${api.minVersion}, library supports up to 0.`));
        }

        keyedVersions[name] = {
          key: api.apiKey,
          name: name,
          version: version
        };
        return keyedVersions;
      }, {});
      cb();
    });
  }

  apiVersions(cb) {
    this.connectExecClose(this.bsHost, this.bsPort, (err, socket, done) => {
      if(err) return cb(err);
      let req = this.kp.request('ApiVersions', {}, this.utilBuffer, 0, (err, result) => {
        done();
        if(err) return cb(err);
        if(err = Orchestrator.handleApiError(result.error)) return cb(err);
        cb(null, result);
      });
      Orchestrator.write(socket, req);
    });
  }

  initTopics(topics, cb) {
    this.metadata(topics, (err, result) => {
      if(err) return cb(err);
      this.brokers = result.brokers.reduce((acc, cur) => {
        acc[cur.nodeId] = cur;
        return acc;
      }, {});
      for(let topic of result.topicMetadata) {
        if(err = Orchestrator.handleApiError(topic.topicError)) return cb(err);

        this.topics[topic.topic] = topic.partitions.map((partition) => {
          return this.brokers[partition.leader];
        });
      }
      cb(null, this.topics);
    });
  }

  connectExecClose(host, port, cb) {
    Orchestrator.openSocket(host, port, (socket) => {
      var done = () => {
        socket.destroy();
      };

      socket.on('error', (err) => {
        done();
        cb(err);
      });

      socket.on('data', this.kp.response);
      cb(null, socket, done);
    });
  }

  utilRequest(apiName, data, cb) {
    let runRequest = (err) => {
      if(err) return cb(err);
      var apiDef = this.versions[apiName];
      this.connectExecClose(this.bsHost, this.bsPort, (err, socket, done) => {
        if(err) return cb(err);
        let req = this.kp.request(apiDef, data, this.utilBuffer, 0, (err, result) => {
          done();
          cb(err, result);
        });
        Orchestrator.write(socket, req);
      });
    }
    if(!this.versions) return this.initApiVersions(runRequest);
    runRequest();
  }

  metadata(topics, cb) {
    this.utilRequest('Metadata', {
      topics: topics.map((topic) => {
        return { topic: topic };
      })
    }, cb);
  }

  topicSocket(topic, cb) {
    this.initTopics([topic], (err, topics) => {
      if(err) return cb(err);
      var host, port;
      // TODO: Support for multiple partitions
      ({ host, port } = topics[topic][0]);
      Orchestrator.openSocket(host, port, (socket) => {
        cb(null, socket);
      });
    });
  }

  request(socket, apiName, data, buffer, offset, cb) {
    var apiDef = this.versions[apiName];
    let request = this.kp.request(apiDef, data, buffer, offset, cb);
    Orchestrator.write(socket, request);
  }

  initStream(stream, cb) {
    if(!stream.socket || stream.socket.destroyed) {
      this.topicSocket(stream.topic, (err, socket) => {
        if(err) return cb(err);
        stream.socket = socket;
        socket.on('data', this.kp.response);
        socket.on('error', (err) => {
          setImmediate(() => {
            stream.emit('error', err);
          });
        });
        stream.on('close', () => stream.socket.end());
        stream.on('finish', () => stream.socket.end());
        cb();
      });
    } else {
      cb();
    }
  }

  static handleApiError(apiErrDef) {
    if(apiErrDef.code !== 0) {
      var err = new ApiError(apiErrDef);
      Error.captureStackTrace(err, Orchestrator.handleApiError);
      return err;
    }
    return false;
  }

  static openSocket(host, port, cb) {
    var socket = net.connect({
      port: port,
      host: host
    }, () => {
      cb(socket);
    });
  }

  static write(socket, data, cb) {
    socket.write(data, 'binary', () => {
      socket.write('\n\n\n\n', 'utf8', cb);
    });
  }
}
