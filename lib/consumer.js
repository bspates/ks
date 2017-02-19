const Readable = require('stream').Readable;
const net = require('net');
const Orchestrator = require('./orchestrator');

module.exports = class Consumer extends Readable {
  constructor(topic, options) {
    super({
      objectMode: true
    });
    this.orc = new Orchestrator(options.clientId, options.host, options.port);
    this.topic = topic;
    this.responseBuffer = Buffer.alloc(16384);
    this.requestBuffer = Buffer.alloc(1024);
    this.index = options.index || 127;
  }

  buildRequest() {
    return {
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1024,
      topics: [
        {
          topic: this.topic,
          partitions: [
            {
              partition: 0,
              fetchOffset: this.index,
              maxBytes: 16384
            }
          ]
        }
      ]
    };
  }

  handleFetchResult(err, result) {
    if(err) return this.emit('error', err);
    for(let response of result.responses) {
      // TODO: Handle multiple partitions
      let partResp = response.partitionResponses[0];
      if(partResp.error.code !== 0) {
        return this.emit('error', response.partitionResponses[0].error);
      }

      if(partResp.recordSet && partResp.recordSet.length && partResp.recordSet.length > 0) {
        this.index += partResp.recordSet.length;
        this.push(partResp.recordSet.map( rec => rec.value ));
      } else {
        setTimeout(() => {
          this.fetch();
        }, 1000);
      }
    }
  }

  fetch() {
    this.orc.initStream(this, (err) => {
      if(err) this.emit('error', err);
      this.orc.request(
        this.socket,
        'Fetch',
        this.buildRequest(),
        this.requestBuffer,
        0,
        this.handleFetchResult
      );
    });
  }

  _read(size) {
    this.fetch();
  }
}
