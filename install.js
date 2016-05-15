var fs = require('fs');
var child_process = require('child_process');

// create jupyter kernel json
var node_path = process.argv[0];
var kernel_path = process.cwd() + '/lib/kernel.js'
var kernel_json = {
	argv: [node_path, kernel_path, "{connection_file}"],
	display_name: "Skale (Node.js)",
	language: "javascript"
}

// create kernel.json in kernelspec directory
var kernel_dir = process.cwd() + '/iskale_kernelspec/';
fs.writeFileSync(kernel_dir + 'kernel.json', JSON.stringify(kernel_json, null, 4));

// install kernelspec directory with jupyter command line
child_process.execSync('jupyter kernelspec install --user --replace ' + kernel_dir);
