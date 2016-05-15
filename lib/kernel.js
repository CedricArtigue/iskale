#!/usr/bin/env node

// Usage: node kernel.js connection_file
var Message = require("jmp").Message;                                   // Jupyter protocol message
var Socket = require("jmp").Socket;                                     // Jupyter protocol socket
var zmq = require("jmp").zmq;                                           // ZMQ bindings
var SkaleREPL = require('./skale.js').SkaleREPL;                        // Skale REPL

var dict = JSON.parse(require("fs").readFileSync(process.argv[2]));     // JSON dictionary from jupyter
var protocolVersion = "5.0";                                            // only for Jupyter 5.0
var iSkaleVersion = "1.0.0";                                            // TODO: get it from package.json

function Kernel() {
    var scheme = dict.signature_scheme.slice("hmac-".length);
    var address = "tcp://" + dict.ip + ":";

    this.repl = new SkaleREPL();
    this.executionCount = 0;

    // HeartBeat socket
    this.hbSocket = zmq.createSocket("rep");
    this.hbSocket.bind(address + dict.hb_port);
    this.hbSocket.on("message", this.hbSocket.send);

    // IOPub socket
    this.iopubSocket = new Socket("pub", scheme, dict.key);
    this.iopubSocket.bind(address + dict.iopub_port);

    // Shell socket
    this.shellSocket = new Socket("router", scheme, dict.key);
    this.shellSocket.bind(address + dict.shell_port);
    this.shellSocket.on("message", function (msg) {
        var msg_type = msg.header.msg_type;
        if (msg_type == 'kernel_info_request') this.kernel_info_request(msg);           // try catch to handle errors
        else if (msg_type == 'execute_request') this.execute_request(msg);
        else console.warn("KERNEL: SHELL_SOCKET: Unhandled message type:", msg_type);        
        // else {
        //     if (this.handlers.hasOwnProperty(msg_type)) {
        //         try {this.handlers[msg_type].call(this, msg);}
        //         catch (e) {console.error("KERNEL: Exception in %s handler: %s", msg_type, e.stack);}
        //     } else console.warn("KERNEL: SHELL_SOCKET: Unhandled message type:", msg_type);
        // }
    }.bind(this));

    // Control socket
    this.controlSocket = new Socket("router", scheme, dict.key);
    this.controlSocket.bind(address + dict.control_port);
    this.controlSocket.on("message", function (msg) {
        var msg_type = msg.header.msg_type;
        if (msg_type === "shutdown_request") this.shutdown_request(msg);
        else console.warn("KERNEL: CONTROL: Unhandled message type:", msg_type);
    }.bind(this));
}

Kernel.prototype.status_busy = function(request) {
    request.respond(this.iopubSocket, 'status', {execution_state: 'busy'});
}

Kernel.prototype.status_idle = function(request) {
    request.respond(this.iopubSocket, 'status', {execution_state: 'idle'});
}

Kernel.prototype.kernel_info_request = function(request) {
    this.status_busy(request);

    request.respond(this.shellSocket, "kernel_info_reply", {
        protocol_version: protocolVersion,
        implementation: "iSkale",
        implementation_version: iSkaleVersion,
        language_info: {
            name: "javascript",
            version: process.versions.node,
            mimetype: "application/javascript",
            file_extension: ".js",
        },
        banner: "ISkale v" + iSkaleVersion + "\nhttps://github.com/skale/skale-engine\n",
        help_links: [{text: "Skale Homepage", url: "https://skale.me"}],
    }, {}, protocolVersion);

    this.status_idle(request);    
}

Kernel.prototype.execute_request = function(request) {
    this.repl.execute(request.content.code, {
        beforeRun: function () {
            this.status_busy(request);
            request.respond(this.iopubSocket, "execute_input", {execution_count: ++this.executionCount, code: request.content.code});
        }.bind(this),
        onStdout: function (data) {
            request.respond(this.iopubSocket, "stream", {name: "stdout", text: data.toString()});
        }.bind(this),
        onStderr: function (data) {
            request.respond(this.iopubSocket, "stream", {name: "stderr", text: data.toString()});
        }.bind(this),
        onSuccess: function (result) {
            request.respond(this.shellSocket, "execute_reply", {
                status: "ok",
                execution_count: this.executionCount,
                payload: [],            // TODO(NR) not implemented,
                user_expressions: {}    // TODO(NR) not implemented,
            });
            request.respond(this.iopubSocket, "execute_result", {
                execution_count: this.executionCount, 
                data: result.mime,
                metadata: {}
            });
        }.bind(this),
        onError: function (result) {
            request.respond(this.shellSocket, "execute_reply", {
                status: "error",
                execution_count: this.executionCount,
                ename: result.error.ename,
                evalue: result.error.evalue,
                traceback: result.error.traceback
            });
            request.respond(this.iopubSocket, "error", {
                execution_count: this.executionCount,
                ename: result.error.ename,
                evalue: result.error.evalue,
                traceback: result.error.traceback
            });
        }.bind(this),
        afterRun: function () {this.status_idle(request);}.bind(this)
    });
}

Kernel.prototype.shutdown_request = function(request) {
    this.status_busy(request);

    function respond(code, signal) {request.respond(this.controlSocket, "shutdown_reply", request.content);}

    if (request.content.restart) this.repl.restart("SIGTERM", respond.bind(this));
    else {
        this.controlSocket.removeAllListeners();
        this.shellSocket.removeAllListeners();
        this.iopubSocket.removeAllListeners();
        this.hbSocket.removeAllListeners();
        this.repl.kill("SIGTERM", function(code, signal) {
            respond.call(this, code, signal);   // send signal before closing sockets
            this.controlSocket.close();
            this.shellSocket.close();
            this.iopubSocket.close();
            this.hbSocket.close();
        }.bind(this));        
    }
    this.status_idle(request);
}

var kernel = new Kernel();

process.on("SIGINT", function() {kernel.repl.restart("SIGTERM");});
