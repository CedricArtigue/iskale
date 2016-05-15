//#!/usr/bin/env node 

const vm = require('vm');

function log(data) {require('fs').appendFileSync('/tmp/log', JSON.stringify(data) + "\n");}

const done = function (result) {
    process.send({mime: {"text/plain": "undefined"}});
}

// initialize VM context
vm.runInThisContext('(function (_require, _done) {require = _require; co = require("co"); $$done$$ = _done;          })')(require, done);

// On parent socket, we receive code to execute, we send a result at completion
// Asynchronous callback must use the $$done$$ function, or yield/await can be used.

process.on('message', msg => {
    var ret;
    log(msg);
    // If yield statement is found, encapsulate code in a co section.
    // Variables initialized with a yield are forced global to be reused between cells.
    // FIXME: other global vars in the original code should be also forced globals.
    //        They are not detected for now. Parsing code (see esprima) is required.
    
    if (msg.code.match(/\W*yield\s+/)) {
        msg.code = 'co(function *() {' +
            msg.code.replace(/\s*var\s*(\w+)\s*=\s*yield\s/g, (match, p1) => ';' + p1 + '=yield ') +
            ';$$done$$();}).catch(err => null);';
    }   
        
    if (msg.code) ret = vm.runInThisContext(msg.code);
    log(ret);
    if (!msg.code.match(/\$\$done\$\$/)) done();
});