const gremlinHelper = require("./gremlinHelper");

module.exports = {
	generateContainerScript(data, logger, cb, app) {
		let { collections, relationships, jsonData, containerData } = data;
		logger.clear();
		try {
			const _ = app.require('lodash');
			const helper = gremlinHelper(_);
			let resultScript = '';

			collections = collections.map(JSON.parse);
			relationships = relationships.map(JSON.parse);

			const verticesScript = helper.generateVertices(collections, jsonData);
			const edgesScript = helper.generateEdges(collections, relationships, jsonData);

			if (verticesScript) {
				resultScript += verticesScript;
			}

			if (edgesScript) {
				resultScript += '\n\n' + edgesScript;
			}

			cb(null, resultScript);
		} catch(e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Forward-Engineering Error');

			cb({ message: e.message, stack: e.stack });
		}
	}
};
