var child_process = require('child_process');
var fs = require('fs');

fs.writeFileSync('/tmp/log', '')
var log = function(data) {fs.appendFileSync('/tmp/log', JSON.stringify(data) + "\n");}

function SkaleREPL() {
    var queue = this.queue = [], self = this;
    this.busy = false;

    var sandbox_source_code = fs.readFileSync(__dirname + '/sandbox.js');
    var child = this.child = child_process.spawn(process.argv[0], ["--eval", sandbox_source_code], {
        stdio: ['ignore', 'pipe', 'pipe', 'ipc' ]
    });

    child.on('error', log);
    child.on('exit', log);
    child.on('close', log);
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', function(data) {queue[0] && queue[0].jcbk.onStdout(data);});
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function(data) {queue[0] && queue[0].jcbk.onStderr(data);});

    child.on('message', function (msg) {
        var jcbk = queue[0].jcbk;
        if (msg.status == 'success') jcbk.onSuccess(msg.data);
        // else if (msg.status == 'error')
        //     jcbk.onError({error: {ename: 'test', evalue: 'test', traceback: ''}});  // custom error
        else return;
        jcbk.afterRun();
        queue.shift();
        if (queue.length) {
            queue[0].jcbk.beforeRun();
            child.send({code: queue[0].code});
        } else self.busy = false;

    });
}

SkaleREPL.prototype.execute = function(code, jcbk) {
    this.queue.push({code: code, jcbk: jcbk});        // Push new execution request inside queue
    if (this.busy) return;                            // kernel currently running
    this.busy = true;
    jcbk.beforeRun();
    this.child.send({code});
}

SkaleREPL.prototype.restart = function(signal, done) {
    this.kill(signal || "SIGTERM", function(code, signal) {
        SkaleREPL.call(this);
        if (done) done(code, signal);
    }.bind(this));
};

SkaleREPL.prototype.kill = function(signal, done) {
    this.child.removeAllListeners();
    this.child.kill(signal || "SIGTERM");
    this.child.on("exit", function(code, signal) {
        if (done) done(code, signal);
    });
};

module.exports = {SkaleREPL: SkaleREPL};