const fs = require('fs');
const ssh = require('tunnel-ssh');
const gremlin = require('gremlin');

let graphName = 'g';

let connection = null;

const getSshConfig = (info) => {
	const config = {
		ssh: info.ssh,
		username: info.ssh_user || 'ec2-user',
		host: info.ssh_host,
		port: info.ssh_port || 22,
		dstHost: info.host,
		dstPort: info.port,
		localHost: '127.0.0.1',
		localPort: info.port,
		keepAlive: true
	};

	return Object.assign({}, config, {
		privateKey: fs.readFileSync(info.ssh_key_file),
		passphrase: info.ssh_key_passphrase
	});
};

const connectViaSsh = (info) => new Promise((resolve, reject) => {
	ssh(getSshConfig(info), (err, tunnel) => {
		if (err) {
			reject(err);
		} else {
			resolve({
				tunnel,
				info: Object.assign({}, info, {
					host: '127.0.0.1'
				})
			});
		}
	});
});

const connect = async info => {
	if (connection) {
		return connection;
	}
	let sshTunnel;
	const result = await connectViaSsh(info);

	info = result.info;
	sshTunnel = result.tunnel;
	

	const data = await connectToInstance(info);
	connection = createConnection({ ...data, sshTunnel });

	return connection;
};

const close = () => {
	if (connection) {
		connection.close();
		connection = null;
	}
};

const connectToInstance = async (info) => {
	const host = info.host;
	const port = info.port;
	const clientOptions = {
		traversalSource: graphName,
		rejectUnauthorized: false
	};
	const uri = `wss://${host}:${port}/gremlin`;
	const client = new gremlin.driver.Client(uri, clientOptions);
	const graphSonClient = new gremlin.driver.Client(uri, {
		...clientOptions,
		reader: createPlainGraphSonReader(),
	});

	await Promise.all([
		client.open(),
		graphSonClient.open(),
	]);

	return {
		client,
		graphSonClient,
	};
};

const createPlainGraphSonReader = () => ({
	read(obj) {
		return {
			...obj,
			result: {
				...obj.result,
				data: obj.result?.data?.['@value'],
			},
		};
	}
});

const createConnection = ({ client, graphSonClient, sshTunnel }) => {
	let closed = false;
	
	return {
		testConnection() {
			if (!client) {
				return Promise.reject(new Error('Connection error'));
			}
		
			return this.submit(`${graphName}.V()`);
		},

		async submit(query) {
			return client.submit(query);
		},

		async submitGraphson(query) {
			return graphSonClient.submit(query);
		},

		async close() {
			if (client) {
				await client.close();
			}
			if (graphSonClient) {
				await graphSonClient.close();
			}
			if (sshTunnel) {
				await sshTunnel.close();
			}

			closed = true;
		},

		closed() {
			return closed;
		}
	};
};

module.exports = {
	connect,
	close,
};